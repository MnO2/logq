//! Bound scalar kernels for batch projections. Field positions and argument
//! storage are resolved once; no row map is built for each expression or row.
//! Functions and casts retain the scalar evaluator's semantics. Branches are
//! evaluated lazily per active row, including their three-valued conditions.

use crate::common::types::{Value, Variables, apply_path_to_value, get_value_by_path_expr_scoped};
use crate::execution::batch::{BatchSchema, BatchToRowAdapter, ColumnBatch, TypedColumn};
use crate::execution::types::{
    EvaluateResult, Expression, ExpressionError, ExpressionResult, Formula, Named, Relation, cast_value,
    with_like_regex,
};
use crate::functions::FunctionRegistry;
use crate::functions::registry::ResolvedFunction;
use crate::simd::bitmap::Bitmap;
use crate::simd::padded_vec::PaddedVecBuilder;
use crate::syntax::ast::{CastType, PathExpr, PathSegment};

pub(crate) enum BoundExpression {
    Constant(Value),
    Column {
        index: usize,
        path: PathExpr,
    },
    Function {
        function: Option<ResolvedFunction>,
        arguments: Vec<Self>,
        values: Vec<Value>,
    },
    Cast(Box<Self>, CastType),
    Logic(Box<BoundFormula>),
    Branch(Vec<(BoundFormula, Self)>, Option<Box<Self>>),
}

impl BoundExpression {
    pub(crate) fn supports(expression: &Expression) -> bool {
        match expression {
            Expression::Constant(_) => true,
            Expression::Variable(path) => matches!(
                path.path_segments.first(),
                Some(PathSegment::AttrName(_) | PathSegment::ArrayIndex(_, _))
            ),
            Expression::Function(_, arguments) => arguments
                .iter()
                .all(|argument| matches!(argument, Named::Expression(expression, _) if Self::supports(expression))),
            Expression::Cast(inner, _) => Self::supports(inner),
            Expression::Logic(formula) => BoundFormula::supports(formula),
            Expression::Branch(branches, otherwise) => {
                branches
                    .iter()
                    .all(|(condition, value)| BoundFormula::supports(condition) && Self::supports(value))
                    && otherwise.as_ref().is_none_or(|value| Self::supports(value))
            }
            // Subqueries own streams and execution context; keep that boundary
            // in the row executor rather than invoking a stream per batch row.
            Expression::Subquery(_) => false,
        }
    }

    pub(crate) fn bind(
        expression: &Expression,
        schema: &BatchSchema,
        scope: &Variables,
        registry: &FunctionRegistry,
    ) -> Self {
        match expression {
            Expression::Constant(value) => Self::Constant(value.clone()),
            Expression::Variable(path) => {
                let name = match path.path_segments.first() {
                    Some(PathSegment::AttrName(name) | PathSegment::ArrayIndex(name, _)) => name,
                    _ => unreachable!("planner checks supported expression paths"),
                };
                match schema.names.iter().rposition(|column| column == name) {
                    Some(index) => Self::Column {
                        index,
                        path: path.clone(),
                    },
                    None => Self::Constant(get_value_by_path_expr_scoped(path, 0, scope, None)),
                }
            }
            Expression::Function(name, arguments) => Self::Function {
                // Missing functions remain lazy: unknown-function errors occur
                // only when this occurrence executes, after its arguments.
                function: registry.resolve(name),
                arguments: arguments
                    .iter()
                    .map(|argument| match argument {
                        Named::Expression(expression, _) => Self::bind(expression, schema, scope, registry),
                        Named::Star => unreachable!("planner checks function arguments"),
                    })
                    .collect(),
                values: Vec::with_capacity(arguments.len()),
            },
            Expression::Cast(inner, kind) => {
                Self::Cast(Box::new(Self::bind(inner, schema, scope, registry)), kind.clone())
            }
            Expression::Logic(formula) => Self::Logic(Box::new(BoundFormula::bind(formula, schema, scope, registry))),
            Expression::Branch(branches, otherwise) => Self::Branch(
                branches
                    .iter()
                    .map(|(condition, value)| {
                        (
                            BoundFormula::bind(condition, schema, scope, registry),
                            Self::bind(value, schema, scope, registry),
                        )
                    })
                    .collect(),
                otherwise
                    .as_ref()
                    .map(|value| Box::new(Self::bind(value, schema, scope, registry))),
            ),
            Expression::Subquery(_) => unreachable!("subqueries stay in row execution"),
        }
    }

    pub(crate) fn evaluate(&mut self, batch: &ColumnBatch, row: usize) -> ExpressionResult<Value> {
        match self {
            Self::Constant(value) => Ok(value.clone()),
            Self::Column { index, path } => {
                if matches!(path.path_segments.as_slice(), [PathSegment::AttrName(_)]) {
                    return Ok(BatchToRowAdapter::extract_value(&batch.columns[*index], row));
                }
                // A nested lookup only owns its selected result. Borrow Mixed
                // roots so unrelated object fields/array elements are not cloned.
                let owned;
                let value = match &batch.columns[*index] {
                    TypedColumn::Mixed { data, null, missing } => {
                        if !missing.is_set(row) {
                            &Value::Missing
                        } else if !null.is_set(row) {
                            &Value::Null
                        } else {
                            &data[row]
                        }
                    }
                    column => {
                        owned = BatchToRowAdapter::extract_value(column, row);
                        &owned
                    }
                };
                if let PathSegment::ArrayIndex(_, index) = &path.path_segments[0] {
                    return Ok(match value {
                        Value::Array(values) => values
                            .get(*index)
                            .map(|value| apply_path_to_value(path, 1, value))
                            .unwrap_or(Value::Missing),
                        _ => Value::Missing,
                    });
                }
                Ok(apply_path_to_value(path, 1, value))
            }
            Self::Function {
                function,
                arguments,
                values,
            } => {
                values.clear();
                for argument in arguments {
                    values.push(argument.evaluate(batch, row)?);
                }
                let result = function
                    .as_ref()
                    .ok_or(ExpressionError::UnknownFunction)
                    .and_then(|function| function.call(values));
                // Keep allocation capacity, but not the last row's owned
                // string/object arguments when a later branch skips this call.
                values.clear();
                result
            }
            Self::Cast(inner, kind) => cast_value(inner.evaluate(batch, row)?, kind),
            Self::Logic(formula) => Ok(formula
                .evaluate(batch, row)
                .map_err(|_| ExpressionError::KeyNotFound)?
                .map_or(Value::Null, Value::Boolean)),
            Self::Branch(branches, otherwise) => {
                for (condition, value) in branches {
                    // Match Expression's existing EvaluateError ->
                    // ExpressionError conversion at a formula boundary.
                    if condition
                        .evaluate(batch, row)
                        .map_err(|_| ExpressionError::KeyNotFound)?
                        == Some(true)
                    {
                        return value.evaluate(batch, row);
                    }
                }
                otherwise
                    .as_mut()
                    .map_or(Ok(Value::Null), |value| value.evaluate(batch, row))
            }
        }
    }

    /// A plain column read is total and can be replaced by ownership transfer
    /// after all computed expressions have finished reading the input batch.
    pub(crate) fn direct_column(&self) -> Option<usize> {
        match self {
            Self::Column { index, path } if matches!(path.path_segments.as_slice(), [PathSegment::AttrName(_)]) => {
                Some(*index)
            }
            _ => None,
        }
    }

    /// A deliberately narrow, total kernel: a Float32 root followed by trusted
    /// built-in Plus calls with Float constants. No user function, branch, cast,
    /// Mixed value or integer operation can enter this path.
    pub(crate) fn float_plus_column(&self, batch: &ColumnBatch) -> Option<TypedColumn> {
        let mut constants = Vec::new();
        let mut expression = self;
        let index = loop {
            match expression {
                Self::Function {
                    function: Some(function),
                    arguments,
                    ..
                } if function.is_builtin_plus() => {
                    let [left, Self::Constant(Value::Float(right))] = arguments.as_slice() else {
                        return None;
                    };
                    constants.push(right.into_inner());
                    expression = left;
                }
                other => break other.direct_column()?,
            }
        };
        if constants.is_empty() {
            return None;
        }
        let TypedColumn::Float32 {
            data,
            null: input_null,
            missing: input_missing,
        } = &batch.columns[index]
        else {
            return None;
        };
        let mut output = PaddedVecBuilder::with_capacity(batch.len + 8);
        let mut null = Bitmap::all_set(batch.len);
        let mut missing = Bitmap::all_set(batch.len);
        for row in 0..batch.len {
            if !batch.selection.is_active(row, batch.len) || !input_missing.is_set(row) {
                missing.unset(row);
                output.push(0.0);
            } else if !input_null.is_set(row) {
                null.unset(row);
                output.push(0.0);
            } else {
                let mut value = data[row];
                // Walk from the root outwards and round at every f32 step.
                // Collapsing/reassociating constants changes values near 2^24.
                for constant in constants.iter().rev() {
                    value += constant;
                }
                output.push(value);
            }
        }
        Some(TypedColumn::Float32 {
            data: output.seal(),
            null,
            missing,
        })
    }
}

pub(crate) enum BoundFormula {
    Constant(bool),
    And(Box<Self>, Box<Self>),
    Or(Box<Self>, Box<Self>),
    Not(Box<Self>),
    Predicate(Relation, BoundExpression, BoundExpression),
    IsNull(BoundExpression, bool),
    IsMissing(BoundExpression, bool),
    Expression(BoundExpression),
    Like(BoundExpression, BoundExpression, bool),
    In(BoundExpression, Vec<BoundExpression>, bool),
}

impl BoundFormula {
    pub(crate) fn supports(formula: &Formula) -> bool {
        match formula {
            Formula::Constant(_) => true,
            Formula::And(left, right) | Formula::Or(left, right) => Self::supports(left) && Self::supports(right),
            Formula::Not(inner) => Self::supports(inner),
            Formula::Predicate(_, left, right) | Formula::Like(left, right) | Formula::NotLike(left, right) => {
                BoundExpression::supports(left) && BoundExpression::supports(right)
            }
            Formula::IsNull(inner)
            | Formula::IsNotNull(inner)
            | Formula::IsMissing(inner)
            | Formula::IsNotMissing(inner)
            | Formula::ExpressionPredicate(inner) => BoundExpression::supports(inner),
            Formula::In(value, items) | Formula::NotIn(value, items) => {
                BoundExpression::supports(value) && items.iter().all(BoundExpression::supports)
            }
        }
    }

    pub(crate) fn bind(
        formula: &Formula,
        schema: &BatchSchema,
        scope: &Variables,
        registry: &FunctionRegistry,
    ) -> Self {
        let bind = |expression: &Expression| BoundExpression::bind(expression, schema, scope, registry);
        match formula {
            Formula::Constant(value) => Self::Constant(*value),
            Formula::And(left, right) => Self::And(
                Box::new(Self::bind(left, schema, scope, registry)),
                Box::new(Self::bind(right, schema, scope, registry)),
            ),
            Formula::Or(left, right) => Self::Or(
                Box::new(Self::bind(left, schema, scope, registry)),
                Box::new(Self::bind(right, schema, scope, registry)),
            ),
            Formula::Not(inner) => Self::Not(Box::new(Self::bind(inner, schema, scope, registry))),
            Formula::Predicate(relation, left, right) => Self::Predicate(relation.clone(), bind(left), bind(right)),
            Formula::IsNull(value) => Self::IsNull(bind(value), false),
            Formula::IsNotNull(value) => Self::IsNull(bind(value), true),
            Formula::IsMissing(value) => Self::IsMissing(bind(value), false),
            Formula::IsNotMissing(value) => Self::IsMissing(bind(value), true),
            Formula::ExpressionPredicate(value) => Self::Expression(bind(value)),
            Formula::Like(value, pattern) => Self::Like(bind(value), bind(pattern), false),
            Formula::NotLike(value, pattern) => Self::Like(bind(value), bind(pattern), true),
            Formula::In(value, items) => Self::In(bind(value), items.iter().map(bind).collect(), false),
            Formula::NotIn(value, items) => Self::In(bind(value), items.iter().map(bind).collect(), true),
        }
    }

    pub(crate) fn evaluate(&mut self, batch: &ColumnBatch, row: usize) -> EvaluateResult<Option<bool>> {
        match self {
            Self::Constant(value) => Ok(Some(*value)),
            Self::And(left, right) => {
                let left = left.evaluate(batch, row)?;
                if left == Some(false) {
                    return Ok(Some(false));
                }
                let right = right.evaluate(batch, row)?;
                Ok(match (left, right) {
                    (_, Some(false)) => Some(false),
                    (Some(true), Some(true)) => Some(true),
                    _ => None,
                })
            }
            Self::Or(left, right) => {
                let left = left.evaluate(batch, row)?;
                if left == Some(true) {
                    return Ok(Some(true));
                }
                let right = right.evaluate(batch, row)?;
                Ok(match (left, right) {
                    (_, Some(true)) => Some(true),
                    (Some(false), Some(false)) => Some(false),
                    _ => None,
                })
            }
            Self::Not(inner) => Ok(inner.evaluate(batch, row)?.map(|value| !value)),
            Self::Predicate(relation, left, right) => {
                Ok(relation.compare_ref(&left.evaluate(batch, row)?, &right.evaluate(batch, row)?)?)
            }
            Self::IsNull(value, negated) => Ok(Some((value.evaluate(batch, row)? == Value::Null) != *negated)),
            Self::IsMissing(value, negated) => Ok(Some((value.evaluate(batch, row)? == Value::Missing) != *negated)),
            Self::Expression(value) => Ok(match value.evaluate(batch, row)? {
                Value::Boolean(value) => Some(value),
                Value::Null | Value::Missing => None,
                _ => Some(true),
            }),
            Self::Like(value, pattern, negated) => {
                let value = value.evaluate(batch, row)?;
                let pattern = pattern.evaluate(batch, row)?;
                match (&value, &pattern) {
                    (Value::Null | Value::Missing, _) | (_, Value::Null | Value::Missing) => Ok(None),
                    (Value::String(value), Value::String(pattern)) => Ok(Some(
                        with_like_regex(pattern, |regex| regex.is_match(value))? != *negated,
                    )),
                    _ => Err(ExpressionError::TypeMismatch.into()),
                }
            }
            Self::In(value, items, negated) => {
                let value = value.evaluate(batch, row)?;
                if matches!(value, Value::Null | Value::Missing) {
                    return Ok(None);
                }
                let mut unknown = false;
                for item in items {
                    let item = item.evaluate(batch, row)?;
                    if matches!(item, Value::Null | Value::Missing) {
                        unknown = true;
                    } else if value == item {
                        return Ok(Some(!*negated));
                    }
                }
                Ok(if unknown { None } else { Some(*negated) })
            }
        }
    }
}

/// Physical planning hoists each literal into an independently named scope
/// variable. Compare the resolved trees when locating an aggregate's projection.
/// Only planner-generated literals are substituted; input variables stay paths.
pub(crate) fn resolve_literal_names(expression: &Expression, scope: &Variables) -> Expression {
    fn rewrite_expression(expression: &mut Expression, scope: &Variables) {
        match expression {
            Expression::Variable(path) => {
                if let [PathSegment::AttrName(name)] = path.path_segments.as_slice() {
                    if name.starts_with("const_") {
                        if let Some(value) = scope.get(name) {
                            *expression = Expression::Constant(value.clone());
                        }
                    }
                }
            }
            Expression::Function(_, arguments) => {
                for argument in arguments {
                    if let Named::Expression(expression, _) = argument {
                        rewrite_expression(expression, scope);
                    }
                }
            }
            Expression::Logic(formula) => rewrite_formula(formula, scope),
            Expression::Cast(inner, _) => rewrite_expression(inner, scope),
            Expression::Branch(branches, otherwise) => {
                for (condition, value) in branches {
                    rewrite_formula(condition, scope);
                    rewrite_expression(value, scope);
                }
                if let Some(value) = otherwise {
                    rewrite_expression(value, scope);
                }
            }
            Expression::Constant(_) | Expression::Subquery(_) => {}
        }
    }
    fn rewrite_formula(formula: &mut Formula, scope: &Variables) {
        match formula {
            Formula::And(left, right) | Formula::Or(left, right) => {
                rewrite_formula(left, scope);
                rewrite_formula(right, scope);
            }
            Formula::Not(inner) => rewrite_formula(inner, scope),
            Formula::Predicate(_, left, right) | Formula::Like(left, right) | Formula::NotLike(left, right) => {
                rewrite_expression(left, scope);
                rewrite_expression(right, scope);
            }
            Formula::IsNull(inner)
            | Formula::IsNotNull(inner)
            | Formula::IsMissing(inner)
            | Formula::IsNotMissing(inner)
            | Formula::ExpressionPredicate(inner) => rewrite_expression(inner, scope),
            Formula::In(value, items) | Formula::NotIn(value, items) => {
                rewrite_expression(value, scope);
                for item in items {
                    rewrite_expression(item, scope);
                }
            }
            Formula::Constant(_) => {}
        }
    }
    let mut resolved = expression.clone();
    rewrite_expression(&mut resolved, scope);
    resolved
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::execution::batch::{ColumnType, TypedColumn};
    use crate::simd::bitmap::Bitmap;
    use crate::simd::selection::SelectionVector;
    use std::sync::Arc;

    fn variable(name: &str) -> Expression {
        Expression::Variable(PathExpr::new(vec![PathSegment::AttrName(name.into())]))
    }

    fn invalid_cast() -> Expression {
        Expression::Cast(
            Box::new(Expression::Constant(Value::String("bad".into()))),
            CastType::Int,
        )
    }

    #[test]
    fn bound_calls_keep_custom_names_and_evaluate_arguments_before_null_propagation() {
        use crate::functions::{Arity, FunctionDef, NullHandling};
        let schema = BatchSchema {
            names: vec![],
            types: vec![],
        };
        let batch = ColumnBatch {
            columns: vec![],
            names: vec![],
            selection: SelectionVector::All,
            len: 1,
        };
        let mut registry = FunctionRegistry::new();
        registry
            .register(FunctionDef {
                name: "add".into(),
                arity: Arity::Exact(2),
                null_handling: NullHandling::Custom,
                func: Box::new(|_| Ok(Value::Int(77))),
            })
            .unwrap();
        registry
            .register(FunctionDef {
                name: "propagate".into(),
                arity: Arity::Exact(2),
                null_handling: NullHandling::Propagate,
                func: Box::new(|_| Ok(Value::Int(88))),
            })
            .unwrap();
        let expressions = [
            (
                Expression::Function(
                    "AdD".into(),
                    vec![
                        Named::Expression(Expression::Constant(Value::Null), None),
                        Named::Expression(Expression::Constant(Value::Missing), None),
                    ],
                ),
                Ok(Value::Int(77)),
            ),
            (
                Expression::Function(
                    "propagate".into(),
                    vec![
                        Named::Expression(Expression::Constant(Value::Missing), None),
                        Named::Expression(invalid_cast(), None),
                    ],
                ),
                Err(ExpressionError::TypeMismatch),
            ),
            (
                Expression::Function("unknown".into(), vec![Named::Expression(invalid_cast(), None)]),
                Err(ExpressionError::TypeMismatch),
            ),
            (
                Expression::Branch(
                    vec![(
                        Box::new(Formula::Constant(false)),
                        Box::new(Expression::Function("unknown".into(), vec![])),
                    )],
                    Some(Box::new(Expression::Constant(Value::Int(3)))),
                ),
                Ok(Value::Int(3)),
            ),
        ];
        for (expression, expected) in expressions {
            let mut bound = BoundExpression::bind(&expression, &schema, &Variables::new(), &registry);
            assert_eq!(bound.evaluate(&batch, 0), expected, "{expression:?}");
        }
    }

    #[test]
    fn nested_bound_paths_match_scalar_with_masks_wildcards_and_scope() {
        let root = Value::Object(Box::new(
            [
                ("leaf".into(), Value::Int(7)),
                (
                    "unused".into(),
                    Value::String("large unused payload ".repeat(512).into()),
                ),
                (
                    "array".into(),
                    Value::Array(vec![Value::Int(3), Value::Null, Value::Missing]),
                ),
            ]
            .into_iter()
            .collect(),
        ));
        let inputs = vec![
            root.clone(),
            Value::Array(vec![root.clone()]),
            Value::Null,
            Value::Missing,
            Value::Int(4),
            root.clone(),
            root,
        ];
        let len = inputs.len();
        let mut null = Bitmap::all_set(len);
        null.unset(5);
        let mut missing = Bitmap::all_set(len);
        missing.unset(6);
        let schema = BatchSchema {
            names: vec!["root".into()],
            types: vec![ColumnType::Mixed],
        };
        let batch = ColumnBatch {
            columns: vec![TypedColumn::Mixed {
                data: inputs,
                null,
                missing,
            }],
            names: schema.names.clone(),
            selection: SelectionVector::All,
            len,
        };
        let scope = [("root".into(), Value::Int(99))].into_iter().collect();
        let registry = Arc::new(crate::functions::register_all().unwrap());
        for path in [
            vec![PathSegment::AttrName("root".into())],
            vec![
                PathSegment::AttrName("root".into()),
                PathSegment::AttrName("leaf".into()),
            ],
            vec![
                PathSegment::AttrName("root".into()),
                PathSegment::ArrayIndex("array".into(), 1),
            ],
            vec![
                PathSegment::AttrName("root".into()),
                PathSegment::AttrName("array".into()),
                PathSegment::Wildcard,
            ],
            vec![PathSegment::AttrName("root".into()), PathSegment::WildcardAttr],
            vec![
                PathSegment::ArrayIndex("root".into(), 0),
                PathSegment::AttrName("leaf".into()),
            ],
            vec![PathSegment::ArrayIndex("root".into(), 8)],
        ] {
            let expression = Expression::Variable(PathExpr::new(path));
            let mut bound = BoundExpression::bind(&expression, &schema, &scope, &registry);
            for row in 0..len {
                let variables = [("root".into(), BatchToRowAdapter::extract_value(&batch.columns[0], row))]
                    .into_iter()
                    .collect();
                assert_eq!(
                    bound.evaluate(&batch, row),
                    expression.expression_value_impl(&variables, Some(&scope), &registry),
                    "{expression:?}, row={row}"
                );
            }
        }
    }

    fn assert_scalar_parity(expression: Expression) {
        let registry = Arc::new(crate::functions::register_all().unwrap());
        let inputs = [Value::Int(7), Value::Null, Value::Missing, Value::String("a".into())];
        let schema = BatchSchema {
            names: vec!["x".into()],
            types: vec![ColumnType::Mixed],
        };
        let batch = ColumnBatch {
            columns: vec![TypedColumn::Mixed {
                data: inputs.to_vec(),
                null: Bitmap::all_set(inputs.len()),
                missing: Bitmap::all_set(inputs.len()),
            }],
            names: schema.names.clone(),
            selection: SelectionVector::All,
            len: inputs.len(),
        };
        let mut scope = Variables::new();
        scope.insert("fallback".into(), Value::Int(9));
        // Existing input columns win over scope, including an explicit MISSING.
        scope.insert("x".into(), Value::Int(100));
        let mut bound = BoundExpression::bind(&expression, &schema, &scope, &registry);
        for (row, input) in inputs.into_iter().enumerate() {
            let mut variables = Variables::new();
            variables.insert("x".into(), input);
            let scalar = expression.expression_value_impl(&variables, Some(&scope), &registry);
            let actual = bound.evaluate(&batch, row);
            assert_eq!(actual, scalar, "row={row}, expression={expression:?}");
        }
    }

    #[test]
    fn bound_formula_truth_and_error_conversion_match_scalar_expression() {
        let error = Formula::Predicate(Relation::MoreThan, Box::new(variable("x")), Box::new(invalid_cast()));
        let true_condition = Formula::Constant(true);
        let false_condition = Formula::Constant(false);
        for formula in [
            error.clone(),
            Formula::And(Box::new(false_condition), Box::new(error.clone())),
            Formula::Or(Box::new(true_condition), Box::new(error.clone())),
            Formula::Not(Box::new(Formula::ExpressionPredicate(Box::new(variable("x"))))),
            Formula::Like(
                Box::new(variable("x")),
                Box::new(Expression::Constant(Value::String("%a%".into()))),
            ),
            Formula::NotLike(
                Box::new(variable("x")),
                Box::new(Expression::Constant(Value::String("%a%".into()))),
            ),
            Formula::In(Box::new(variable("x")), vec![variable("x"), invalid_cast()]),
            Formula::NotIn(Box::new(variable("x")), vec![variable("x"), invalid_cast()]),
        ] {
            assert_scalar_parity(Expression::Logic(Box::new(formula)));
        }
        assert_scalar_parity(Expression::Branch(
            vec![(Box::new(error), Box::new(variable("x")))],
            Some(Box::new(variable("fallback"))),
        ));
    }

    #[test]
    fn bound_case_skips_unselected_values_and_preserves_scope() {
        assert_scalar_parity(Expression::Branch(
            vec![
                (Box::new(Formula::Constant(false)), Box::new(invalid_cast())),
                (
                    Box::new(Formula::IsNotMissing(Box::new(variable("x")))),
                    Box::new(variable("x")),
                ),
            ],
            Some(Box::new(variable("fallback"))),
        ));
        assert_scalar_parity(Expression::Branch(
            vec![(Box::new(Formula::Constant(false)), Box::new(invalid_cast()))],
            None,
        ));
    }

    #[test]
    fn separate_aggregate_expression_occurrences_keep_their_own_values() {
        use crate::common::types::DataSource;
        use crate::execution::types::{Aggregate, NamedAggregate, Node, SumAggregate};
        use crate::functions::{Arity, FunctionDef, NullHandling};
        use std::sync::atomic::{AtomicI32, Ordering};
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("input.jsonl");
        std::fs::write(&path, "{}\n").unwrap();
        for threads in [1, 4] {
            let counter = Arc::new(AtomicI32::new(0));
            let calls = counter.clone();
            let mut registry = FunctionRegistry::new();
            registry
                .register(FunctionDef {
                    name: "next_value".into(),
                    arity: Arity::Exact(0),
                    null_handling: NullHandling::Custom,
                    func: Box::new(move |_| Ok(Value::Int(calls.fetch_add(1, Ordering::SeqCst) + 1))),
                })
                .unwrap();
            let expression = Expression::Function("next_value".into(), vec![]);
            let named = Named::Expression(expression, None);
            let node = Node::GroupBy(
                vec![],
                vec![
                    NamedAggregate::new(Aggregate::Sum(SumAggregate::new(), named.clone()), Some("a".into())),
                    NamedAggregate::new(Aggregate::Sum(SumAggregate::new(), named.clone()), Some("b".into())),
                ],
                Box::new(Node::Map(
                    vec![named.clone(), named],
                    Box::new(Node::DataSource(
                        DataSource::File(path.clone(), "jsonl".into(), "it".into()),
                        vec![],
                    )),
                )),
            );
            let mut stream = node.get(Variables::new(), Arc::new(registry), threads).unwrap();
            let record = stream.next().unwrap().unwrap();
            assert_eq!(
                record.get(&PathExpr::new(vec![PathSegment::AttrName("a".into())])),
                Value::Float(ordered_float::OrderedFloat(1.0))
            );
            assert_eq!(
                record.get(&PathExpr::new(vec![PathSegment::AttrName("b".into())])),
                Value::Float(ordered_float::OrderedFloat(2.0))
            );
            assert_eq!(counter.load(Ordering::SeqCst), 2);
        }
    }
}
