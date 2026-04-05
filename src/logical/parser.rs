use super::types;
use crate::common::types as common;
use crate::common::types::ParsingContext;
use crate::execution;
use crate::functions::registry::RegistryError;
use crate::functions::FunctionRegistry;
use crate::logical::types::Named;
use crate::syntax::ast;
use crate::syntax::ast::{FromClause, JoinType, PathExpr, PathSegment, TableReference};
use hashbrown::HashSet;
use std::sync::Arc;

#[derive(thiserror::Error, Debug, Clone, PartialEq, Eq)]
pub enum ParseError {
    #[error("Type Mismatch")]
    TypeMismatch,
    #[error("Not Aggregate Function")]
    NotAggregateFunction,
    #[error("Group By statement but no aggregate function provided")]
    GroupByWithoutAggregateFunction,
    #[error("Group By statement mismatch with the non-aggregate fields")]
    GroupByFieldsMismatch,
    #[error("Invalid Arguments: {0}")]
    InvalidArguments(String),
    #[error("Invalid Arguments: {0}")]
    UnknownFunction(String),
    #[error("Having clause but no Group By clause provided")]
    HavingClauseWithoutGroupBy,
    #[error("Invalid table reference in From Clause")]
    FromClausePathInvalidTableReference,
    #[error("Using 'as' to define an alias is required in From Clause for nested path expr")]
    FromClauseMissingAsForPathExpr,
    #[error("Unknown table '{0}'. Available tables: {1}")]
    UnknownTable(String, String),
    #[error("Stdin cannot be used as the right side of a join (it can only be read once)")]
    StdinInJoinRightSide,
    #[error("SELECT * with GROUP BY is not supported for jsonl or multi-table queries (no fixed schema to expand)")]
    StarGroupByUnsupported,
}

impl From<RegistryError> for ParseError {
    fn from(e: RegistryError) -> Self {
        match e {
            RegistryError::UnknownFunction(name) => ParseError::UnknownFunction(name),
            RegistryError::ArityMismatch { name, expected, actual } => {
                ParseError::InvalidArguments(
                    format!("{} expects {} argument(s), got {}", name, expected, actual),
                )
            }
            RegistryError::DuplicateFunction(name) => {
                ParseError::InvalidArguments(format!("Duplicate function: {}", name))
            }
        }
    }
}

pub type ParseResult<T> = Result<T, ParseError>;

fn parse_prefix_operator(
    ctx: &common::ParsingContext,
    op: types::LogicPrefixOp,
    child: &ast::Expression,
) -> ParseResult<Box<types::Formula>> {
    let child_parsed = parse_logic(ctx, child)?;

    let prefix_op = types::Formula::PrefixOperator(op, child_parsed);
    Ok(Box::new(prefix_op))
}

fn parse_infix_operator(
    ctx: &common::ParsingContext,
    op: types::LogicInfixOp,
    left: &ast::Expression,
    right: &ast::Expression,
) -> ParseResult<Box<types::Formula>> {
    let left_parsed = parse_logic(ctx, left)?;
    let right_parsed = parse_logic(ctx, right)?;

    let infix_op = types::Formula::InfixOperator(op, left_parsed, right_parsed);
    Ok(Box::new(infix_op))
}

fn parse_logic(ctx: &common::ParsingContext, expr: &ast::Expression) -> ParseResult<Box<types::Formula>> {
    match expr {
        ast::Expression::BinaryOperator(op, l, r) => {
            if op == &ast::BinaryOperator::And {
                let formula = parse_infix_operator(ctx, types::LogicInfixOp::And, l, r)?;
                Ok(formula)
            } else if op == &ast::BinaryOperator::Or {
                let formula = parse_infix_operator(ctx, types::LogicInfixOp::Or, l, r)?;
                Ok(formula)
            } else {
                parse_condition(ctx, expr)
            }
        }
        ast::Expression::UnaryOperator(op, c) => {
            if op == &ast::UnaryOperator::Not {
                let formula = parse_prefix_operator(ctx, types::LogicPrefixOp::Not, c)?;
                Ok(formula)
            } else {
                unreachable!()
            }
        }
        ast::Expression::Value(value_expr) => match value_expr {
            ast::Value::Boolean(b) => Ok(Box::new(types::Formula::Constant(*b))),
            _ => Err(ParseError::TypeMismatch),
        },
        ast::Expression::IsNull(inner) => {
            let inner_expr = parse_value_expression(ctx, inner)?;
            Ok(Box::new(types::Formula::IsNull(inner_expr)))
        }
        ast::Expression::IsNotNull(inner) => {
            let inner_expr = parse_value_expression(ctx, inner)?;
            Ok(Box::new(types::Formula::IsNotNull(inner_expr)))
        }
        ast::Expression::IsMissing(inner) => {
            let inner_expr = parse_value_expression(ctx, inner)?;
            Ok(Box::new(types::Formula::IsMissing(inner_expr)))
        }
        ast::Expression::IsNotMissing(inner) => {
            let inner_expr = parse_value_expression(ctx, inner)?;
            Ok(Box::new(types::Formula::IsNotMissing(inner_expr)))
        }
        ast::Expression::Like(inner_expr, pattern) => {
            let left = parse_value_expression(ctx, inner_expr)?;
            let right = parse_value_expression(ctx, pattern)?;
            Ok(Box::new(types::Formula::Like(left, right)))
        }
        ast::Expression::NotLike(inner_expr, pattern) => {
            let left = parse_value_expression(ctx, inner_expr)?;
            let right = parse_value_expression(ctx, pattern)?;
            Ok(Box::new(types::Formula::NotLike(left, right)))
        }
        ast::Expression::In(inner_expr, list) => {
            let expr = parse_value_expression(ctx, inner_expr)?;
            let list_exprs: Vec<types::Expression> = list
                .iter()
                .map(|e| parse_value_expression(ctx, e).map(|boxed| *boxed))
                .collect::<ParseResult<Vec<_>>>()?;
            Ok(Box::new(types::Formula::In(expr, list_exprs)))
        }
        ast::Expression::NotIn(inner_expr, list) => {
            let expr = parse_value_expression(ctx, inner_expr)?;
            let list_exprs: Vec<types::Expression> = list
                .iter()
                .map(|e| parse_value_expression(ctx, e).map(|boxed| *boxed))
                .collect::<ParseResult<Vec<_>>>()?;
            Ok(Box::new(types::Formula::NotIn(expr, list_exprs)))
        }
        ast::Expression::Between(_, _, _) | ast::Expression::NotBetween(_, _, _) => {
            unreachable!("BETWEEN/NOT BETWEEN should be desugared before reaching the logical planner")
        }
        ast::Expression::Cast(inner, cast_type) => {
            let expr = parse_value_expression(ctx, &ast::Expression::Cast(inner.clone(), cast_type.clone()))?;
            Ok(Box::new(types::Formula::ExpressionPredicate(expr)))
        }
        ast::Expression::Subquery(_) => {
            let expr = parse_value_expression(ctx, expr)?;
            Ok(Box::new(types::Formula::ExpressionPredicate(expr)))
        }
        ast::Expression::FuncCall(_, _, _)
        | ast::Expression::CaseWhenExpression(_)
        | ast::Expression::Column(_) => {
            // Expression used in boolean context — treat as a condition
            // by wrapping in an ExpressionPredicate
            let expr = parse_value_expression(ctx, expr)?;
            Ok(Box::new(types::Formula::ExpressionPredicate(expr)))
        }
    }
}

fn parse_logic_expression(ctx: &common::ParsingContext, expr: &ast::Expression) -> ParseResult<Box<types::Expression>> {
    let formula = parse_logic(ctx, expr)?;
    Ok(Box::new(types::Expression::Logic(formula)))
}

fn parse_value(value: &ast::Value) -> ParseResult<Box<types::Expression>> {
    match value {
        ast::Value::Boolean(b) => Ok(Box::new(types::Expression::Constant(common::Value::Boolean(*b)))),
        ast::Value::Float(f) => Ok(Box::new(types::Expression::Constant(common::Value::Float(*f)))),
        ast::Value::Integral(i) => Ok(Box::new(types::Expression::Constant(common::Value::Int(*i)))),
        ast::Value::StringLiteral(s) => Ok(Box::new(types::Expression::Constant(common::Value::String(s.clone())))),
        ast::Value::Null => Ok(Box::new(types::Expression::Constant(common::Value::Null))),
        ast::Value::Missing => Ok(Box::new(types::Expression::Constant(common::Value::Missing))),
    }
}

fn parse_case_when_expression(
    ctx: &common::ParsingContext,
    value_expr: &ast::Expression,
) -> ParseResult<Box<types::Expression>> {
    match value_expr {
        ast::Expression::CaseWhenExpression(case_when_expr) => {
            let mut branches = Vec::new();
            for (condition, then_expr) in &case_when_expr.branches {
                let formula = parse_logic(ctx, condition)?;
                let expr = parse_value_expression(ctx, then_expr)?;
                branches.push((formula, expr));
            }
            let else_expr = if let Some(e) = &case_when_expr.else_expr {
                Some(parse_value_expression(ctx, e)?)
            } else {
                None
            };

            Ok(Box::new(types::Expression::Branch(branches, else_expr)))
        }
        _ => {
            unreachable!();
        }
    }
}

fn parse_binary_operator(
    ctx: &common::ParsingContext,
    value_expr: &ast::Expression,
) -> ParseResult<Box<types::Expression>> {
    match value_expr {
        ast::Expression::BinaryOperator(op, left_expr, right_expr) => {
            if op == &ast::BinaryOperator::And || op == &ast::BinaryOperator::Or {
                parse_logic_expression(ctx, value_expr)
            } else if op == &ast::BinaryOperator::Equal
                || op == &ast::BinaryOperator::NotEqual
                || op == &ast::BinaryOperator::MoreThan
                || op == &ast::BinaryOperator::LessThan
                || op == &ast::BinaryOperator::GreaterEqual
                || op == &ast::BinaryOperator::LessEqual
            {
                let formula = parse_condition(ctx, value_expr)?;
                Ok(Box::new(types::Expression::Logic(formula)))
            } else {
                let func_name = (*op).to_string();
                let left = parse_value_expression(ctx, left_expr)?;
                let right = parse_value_expression(ctx, right_expr)?;
                let args = vec![
                    types::Named::Expression(*left, None),
                    types::Named::Expression(*right, None),
                ];
                Ok(Box::new(types::Expression::Function(func_name, args)))
            }
        }
        _ => {
            unreachable!();
        }
    }
}

fn parse_unary_operator(ctx: &ParsingContext, value_expr: &ast::Expression) -> ParseResult<Box<types::Expression>> {
    match value_expr {
        ast::Expression::UnaryOperator(op, expr) => {
            if op == &ast::UnaryOperator::Not {
                let formula = parse_prefix_operator(ctx, types::LogicPrefixOp::Not, expr)?;
                Ok(Box::new(types::Expression::Logic(formula)))
            } else {
                unreachable!();
            }
        }
        _ => {
            unreachable!();
        }
    }
}

fn parse_value_expression(
    ctx: &common::ParsingContext,
    value_expr: &ast::Expression,
) -> ParseResult<Box<types::Expression>> {
    match value_expr {
        ast::Expression::Value(v) => {
            let expr = parse_value(v)?;
            Ok(expr)
        }
        ast::Expression::Column(path_expr) => Ok(Box::new(types::Expression::Variable(path_expr.clone()))),
        ast::Expression::BinaryOperator(_, _, _) => parse_binary_operator(ctx, value_expr),
        ast::Expression::UnaryOperator(_, _) => parse_unary_operator(ctx, value_expr),
        ast::Expression::FuncCall(func_name, select_exprs, _) => {
            // Validate scalar functions at planning time.
            // Aggregate functions are handled by parse_aggregate() and should not
            // be validated as scalar functions.
            if !is_aggregate_name(func_name) {
                ctx.registry.validate(func_name, select_exprs.len())
                    .map_err(ParseError::from)?;
            }

            let mut args = Vec::new();
            for select_expr in select_exprs.iter() {
                let arg = parse_expression(ctx, select_expr)?;
                args.push(*arg);
            }
            Ok(Box::new(types::Expression::Function(func_name.clone(), args)))
        }
        ast::Expression::CaseWhenExpression(_) => parse_case_when_expression(ctx, value_expr),
        ast::Expression::IsNull(inner) => {
            let inner_expr = parse_value_expression(ctx, inner)?;
            let formula = Box::new(types::Formula::IsNull(inner_expr));
            Ok(Box::new(types::Expression::Logic(formula)))
        }
        ast::Expression::IsNotNull(inner) => {
            let inner_expr = parse_value_expression(ctx, inner)?;
            let formula = Box::new(types::Formula::IsNotNull(inner_expr));
            Ok(Box::new(types::Expression::Logic(formula)))
        }
        ast::Expression::IsMissing(inner) => {
            let inner_expr = parse_value_expression(ctx, inner)?;
            let formula = Box::new(types::Formula::IsMissing(inner_expr));
            Ok(Box::new(types::Expression::Logic(formula)))
        }
        ast::Expression::IsNotMissing(inner) => {
            let inner_expr = parse_value_expression(ctx, inner)?;
            let formula = Box::new(types::Formula::IsNotMissing(inner_expr));
            Ok(Box::new(types::Expression::Logic(formula)))
        }
        ast::Expression::Like(inner_expr, pattern) => {
            let left = parse_value_expression(ctx, inner_expr)?;
            let right = parse_value_expression(ctx, pattern)?;
            let formula = Box::new(types::Formula::Like(left, right));
            Ok(Box::new(types::Expression::Logic(formula)))
        }
        ast::Expression::NotLike(inner_expr, pattern) => {
            let left = parse_value_expression(ctx, inner_expr)?;
            let right = parse_value_expression(ctx, pattern)?;
            let formula = Box::new(types::Formula::NotLike(left, right));
            Ok(Box::new(types::Expression::Logic(formula)))
        }
        ast::Expression::In(inner_expr, list) => {
            let expr = parse_value_expression(ctx, inner_expr)?;
            let list_exprs: Vec<types::Expression> = list
                .iter()
                .map(|e| parse_value_expression(ctx, e).map(|boxed| *boxed))
                .collect::<ParseResult<Vec<_>>>()?;
            let formula = Box::new(types::Formula::In(expr, list_exprs));
            Ok(Box::new(types::Expression::Logic(formula)))
        }
        ast::Expression::NotIn(inner_expr, list) => {
            let expr = parse_value_expression(ctx, inner_expr)?;
            let list_exprs: Vec<types::Expression> = list
                .iter()
                .map(|e| parse_value_expression(ctx, e).map(|boxed| *boxed))
                .collect::<ParseResult<Vec<_>>>()?;
            let formula = Box::new(types::Formula::NotIn(expr, list_exprs));
            Ok(Box::new(types::Expression::Logic(formula)))
        }
        ast::Expression::Between(_, _, _) | ast::Expression::NotBetween(_, _, _) => {
            unreachable!("BETWEEN/NOT BETWEEN should be desugared before reaching the logical planner")
        }
        ast::Expression::Cast(inner, cast_type) => {
            let expr = parse_value_expression(ctx, inner)?;
            Ok(Box::new(types::Expression::Cast(expr, cast_type.clone())))
        }
        ast::Expression::Subquery(stmt) => {
            let inner_node = parse_query(*stmt.clone(), ctx.data_sources.clone(), ctx.registry.clone())?;
            Ok(Box::new(types::Expression::Subquery(Box::new(inner_node))))
        }
    }
}

fn parse_relation(op: &ast::BinaryOperator) -> ParseResult<types::Relation> {
    match op {
        ast::BinaryOperator::Equal => Ok(types::Relation::Equal),
        ast::BinaryOperator::NotEqual => Ok(types::Relation::NotEqual),
        ast::BinaryOperator::GreaterEqual => Ok(types::Relation::GreaterEqual),
        ast::BinaryOperator::LessEqual => Ok(types::Relation::LessEqual),
        ast::BinaryOperator::LessThan => Ok(types::Relation::LessThan),
        ast::BinaryOperator::MoreThan => Ok(types::Relation::MoreThan),
        _ => unreachable!(),
    }
}

fn parse_condition(ctx: &common::ParsingContext, condition: &ast::Expression) -> ParseResult<Box<types::Formula>> {
    match condition {
        ast::Expression::BinaryOperator(op, left_expr, right_expr) => {
            let left = parse_value_expression(ctx, left_expr)?;
            let right = parse_value_expression(ctx, right_expr)?;
            let rel_op = parse_relation(op)?;
            Ok(Box::new(types::Formula::Predicate(rel_op, left, right)))
        }
        _ => {
            // Non-comparison expression in condition context (e.g., a function call
            // or column reference used as a boolean predicate)
            let expr = parse_value_expression(ctx, condition)?;
            Ok(Box::new(types::Formula::ExpressionPredicate(expr)))
        }
    }
}

fn parse_expression(ctx: &ParsingContext, select_expr: &ast::SelectExpression) -> ParseResult<Box<types::Named>> {
    match select_expr {
        ast::SelectExpression::Star => Ok(Box::new(types::Named::Star)),
        ast::SelectExpression::Expression(expr, name_opt) => {
            let e = parse_value_expression(ctx, expr)?;
            match &*e {
                types::Expression::Variable(path_expr) => {
                    let name = match path_expr.path_segments.last().unwrap() {
                        PathSegment::ArrayIndex(_s, _) => None,
                        PathSegment::AttrName(s) => Some(s.clone()),
                        PathSegment::Wildcard | PathSegment::WildcardAttr => None,
                    };

                    Ok(Box::new(types::Named::Expression(*e.clone(), name)))
                }
                _ => Ok(Box::new(types::Named::Expression(*e, name_opt.clone()))),
            }
        }
    }
}

fn from_str(value: &str, named: types::Named) -> ParseResult<types::Aggregate> {
    match value {
        "avg" => Ok(types::Aggregate::Avg(named)),
        "count" => Ok(types::Aggregate::Count(named)),
        "first" => Ok(types::Aggregate::First(named)),
        "last" => Ok(types::Aggregate::Last(named)),
        "max" => Ok(types::Aggregate::Max(named)),
        "min" => Ok(types::Aggregate::Min(named)),
        "sum" => Ok(types::Aggregate::Sum(named)),
        "approx_count_distinct" => Ok(types::Aggregate::ApproxCountDistinct(named)),
        _ => Err(ParseError::NotAggregateFunction),
    }
}

/// Returns true if the given function name is an aggregate function.
fn is_aggregate_name(name: &str) -> bool {
    match name.to_ascii_lowercase().as_str() {
        // from_str() names:
        "avg" | "count" | "first" | "last" | "max" | "min" | "sum"
        | "approx_count_distinct"
        // within_group path names:
        | "percentile_disc" | "approx_percentile"
        // group_as:
        | "group_as" => true,
        _ => false,
    }
}

fn parse_ordering(ordering: ast::Ordering) -> ParseResult<types::Ordering> {
    match ordering {
        ast::Ordering::Desc => Ok(types::Ordering::Desc),
        ast::Ordering::Asc => Ok(types::Ordering::Asc),
    }
}

fn parse_aggregate(ctx: &ParsingContext, select_expr: &ast::SelectExpression) -> ParseResult<types::NamedAggregate> {
    match select_expr {
        ast::SelectExpression::Expression(expr, name_opt) => match &**expr {
            ast::Expression::FuncCall(func_name, args, within_group_opt) => {
                let named = *parse_expression(ctx, &args[0])?;

                let aggregate = if let Some(within_group_clause) = within_group_opt {
                    match named {
                        types::Named::Expression(expr, _) => match expr {
                            types::Expression::Constant(val) => match val {
                                common::Value::Float(f) => {
                                    let o = parse_ordering(within_group_clause.ordering_term.ordering.clone())?;

                                    if func_name == "percentile_disc" {
                                        types::Aggregate::PercentileDisc(
                                            f,
                                            within_group_clause.ordering_term.column_name.clone(),
                                            o,
                                        )
                                    } else if func_name == "approx_percentile" {
                                        types::Aggregate::ApproxPercentile(
                                            f,
                                            within_group_clause.ordering_term.column_name.clone(),
                                            o,
                                        )
                                    } else {
                                        return Err(ParseError::UnknownFunction(func_name.to_string()));
                                    }
                                }
                                _ => {
                                    return Err(ParseError::InvalidArguments("percentile_disc".to_string()));
                                }
                            },
                            _ => {
                                //FIXME: should be ok for a function returning Float as well
                                return Err(ParseError::InvalidArguments("percentile_disc".to_string()));
                            }
                        },
                        _ => {
                            //Star should be disallowed.
                            return Err(ParseError::InvalidArguments("percentile_disc".to_string()));
                        }
                    }
                } else {
                    from_str(&**func_name, named)?
                };
                let named_aggregate = types::NamedAggregate::new(aggregate, name_opt.clone());
                Ok(named_aggregate)
            }
            _ => Err(ParseError::TypeMismatch),
        },
        _ => Err(ParseError::TypeMismatch),
    }
}

fn check_contains_group_as_var(named_list: &[types::Named], group_as_var: &String) -> bool {
    for named in named_list.iter() {
        match named {
            types::Named::Expression(expr, var_name_opt) => {
                if let Some(var_name) = var_name_opt {
                    if var_name.eq(group_as_var) {
                        return true;
                    }
                } else if let types::Expression::Variable(var_name) = expr {
                    let path_expr = &PathExpr::new(vec![PathSegment::AttrName(group_as_var.clone())]);
                    if path_expr.eq(var_name) {
                        return true;
                    }
                }
            }
            types::Named::Star => {}
        }
    }

    false
}

fn check_env_ref(data_sources: &common::DataSourceRegistry, table_reference: &TableReference) -> ParseResult<()> {
    let path_expr = &table_reference.path_expr;
    if path_expr.path_segments.is_empty() {
        return Err(ParseError::FromClausePathInvalidTableReference);
    }

    match &path_expr.path_segments[0] {
        PathSegment::AttrName(s) => {
            if !data_sources.contains_key(s) {
                let mut available: Vec<_> = data_sources.keys().cloned().collect();
                available.sort();
                return Err(ParseError::UnknownTable(s.clone(), available.join(", ")));
            }
        }
        _ => return Err(ParseError::FromClausePathInvalidTableReference),
    }

    if path_expr.path_segments.len() > 1 && table_reference.as_clause.is_none() {
        return Err(ParseError::FromClauseMissingAsForPathExpr);
    }
    Ok(())
}

fn check_env(data_sources: &common::DataSourceRegistry, from_clause: &FromClause) -> ParseResult<()> {
    let refs = from_clause.collect_table_references();
    for table_reference in refs {
        check_env_ref(data_sources, table_reference)?;
    }
    Ok(())
}

fn to_bindings_for_ref(data_sources: &common::DataSourceRegistry, table_reference: &TableReference) -> Vec<common::Binding> {
    let path_expr = match &table_reference.path_expr.path_segments[0] {
        PathSegment::AttrName(s) => {
            if data_sources.contains_key(s) {
                PathExpr::new(
                    table_reference.path_expr.path_segments.iter().skip(1).cloned().collect(),
                )
            } else {
                table_reference.path_expr.clone()
            }
        }
        _ => table_reference.path_expr.clone(),
    };

    if let Some(name) = table_reference.as_clause.clone() {
        vec![common::Binding {
            path_expr,
            name,
            idx_name: table_reference.at_clause.clone(),
        }]
    } else {
        vec![]
    }
}

fn to_bindings(data_sources: &common::DataSourceRegistry, table_references: &Vec<TableReference>) -> Vec<common::Binding> {
    table_references
        .iter()
        .flat_map(|table_reference| to_bindings_for_ref(data_sources, table_reference))
        .collect()
}

fn check_group_by_vars(named: &Named, group_by_vars: &HashSet<String>) -> bool {
    match named {
        Named::Expression(expr, alias) => {
            match expr {
                types::Expression::Variable(path_expr) => match path_expr.path_segments.last().unwrap() {
                    PathSegment::AttrName(s) => {
                        return group_by_vars.contains(s);
                    }
                    PathSegment::ArrayIndex(_, _)
                    | PathSegment::Wildcard
                    | PathSegment::WildcardAttr => {
                        return false;
                    }
                },
                types::Expression::Function(_, _) => {
                    if let Some(a) = alias {
                        return group_by_vars.contains(a);
                    } else {
                        return false;
                    }
                }
                _ => {
                    //TODO: branch furhter
                    return false;
                }
            }
        }
        _ => {
            return false;
        }
    }
}

fn lookup_data_source(
    data_sources: &common::DataSourceRegistry,
    table_ref: &TableReference,
) -> ParseResult<common::DataSource> {
    match &table_ref.path_expr.path_segments[0] {
        PathSegment::AttrName(name) => {
            data_sources.get(name).cloned().ok_or_else(|| {
                let mut available: Vec<_> = data_sources.keys().cloned().collect();
                available.sort();
                ParseError::UnknownTable(name.clone(), available.join(", "))
            })
        }
        _ => Err(ParseError::FromClausePathInvalidTableReference),
    }
}

fn extract_base_name(table_ref: &TableReference) -> ParseResult<String> {
    match &table_ref.path_expr.path_segments[0] {
        PathSegment::AttrName(s) => Ok(s.clone()),
        _ => Err(ParseError::FromClausePathInvalidTableReference),
    }
}

fn build_from_node(
    ctx: &common::ParsingContext,
    from_clause: &FromClause,
) -> ParseResult<types::Node> {
    match from_clause {
        FromClause::Tables(table_references) => {
            if table_references.len() == 1 {
                let ref0 = &table_references[0];
                let ds = lookup_data_source(&ctx.data_sources, ref0)?;
                let bindings = to_bindings_for_ref(&ctx.data_sources, ref0);
                Ok(types::Node::DataSource(ds, bindings))
            } else {
                let ref0 = &table_references[0];
                let ds0 = lookup_data_source(&ctx.data_sources, ref0)?;
                let first_bindings = to_bindings_for_ref(&ctx.data_sources, ref0);
                let mut node = types::Node::DataSource(ds0, first_bindings);
                for table_ref in table_references.iter().skip(1) {
                    let ds = lookup_data_source(&ctx.data_sources, table_ref)?;
                    if matches!(&ds, common::DataSource::Stdin(..)) {
                        return Err(ParseError::StdinInJoinRightSide);
                    }
                    let ref_bindings = to_bindings_for_ref(&ctx.data_sources, table_ref);
                    let right = types::Node::DataSource(ds, ref_bindings);
                    node = types::Node::CrossJoin(Box::new(node), Box::new(right));
                }
                Ok(node)
            }
        }
        FromClause::Join { left, right, join_type, condition } => {
            let left_node = build_from_node(ctx, left)?;
            let ds_right = lookup_data_source(&ctx.data_sources, right)?;
            if matches!(&ds_right, common::DataSource::Stdin(..)) {
                return Err(ParseError::StdinInJoinRightSide);
            }
            let right_bindings = to_bindings_for_ref(&ctx.data_sources, right);
            let right_node = types::Node::DataSource(ds_right, right_bindings);
            match join_type {
                JoinType::Cross => {
                    Ok(types::Node::CrossJoin(Box::new(left_node), Box::new(right_node)))
                }
                JoinType::Left => {
                    let on_expr = condition.as_ref().expect("LEFT JOIN requires ON condition");
                    let formula = parse_logic(ctx, on_expr)?;
                    Ok(types::Node::LeftJoin(Box::new(left_node), Box::new(right_node), formula))
                }
            }
        }
    }
}

pub(crate) fn parse_query_top(q: ast::Query, data_sources: common::DataSourceRegistry, registry: Arc<FunctionRegistry>) -> ParseResult<types::Node> {
    match q {
        ast::Query::Select(stmt) => parse_query(stmt, data_sources, registry),
        ast::Query::SetOp { op, all, left, right } => {
            let left_node = parse_query_top(*left, data_sources.clone(), registry.clone())?;
            let right_node = parse_query_top(*right, data_sources, registry)?;
            match op {
                ast::SetOperator::Union => {
                    let union_node = types::Node::Union(Box::new(left_node), Box::new(right_node));
                    if all {
                        Ok(union_node)
                    } else {
                        Ok(types::Node::Distinct(Box::new(union_node)))
                    }
                }
                ast::SetOperator::Intersect => {
                    Ok(types::Node::Intersect(Box::new(left_node), Box::new(right_node), all))
                }
                ast::SetOperator::Except => {
                    Ok(types::Node::Except(Box::new(left_node), Box::new(right_node), all))
                }
            }
        }
    }
}

pub(crate) fn parse_query(query: ast::SelectStatement, data_sources: common::DataSourceRegistry, registry: Arc<FunctionRegistry>) -> ParseResult<types::Node> {
    let from_clause = &query.from_clause;

    check_env(&data_sources, from_clause)?;

    // Extract file_format from the primary (leftmost) table
    let primary_table_name = {
        fn leftmost_name(fc: &FromClause) -> ParseResult<String> {
            match fc {
                FromClause::Tables(refs) => extract_base_name(&refs[0]),
                FromClause::Join { left, .. } => leftmost_name(left),
            }
        }
        leftmost_name(from_clause)?
    };
    let primary_ds = data_sources.get(&primary_table_name).unwrap();
    let file_format = match primary_ds {
        common::DataSource::File(_, fmt, _) => fmt.clone(),
        common::DataSource::Stdin(fmt, _) => fmt.clone(),
    };

    let parsing_context = common::ParsingContext {
        data_sources: data_sources.clone(),
        registry: registry.clone(),
    };

    let mut root = build_from_node(&parsing_context, from_clause)?;
    let mut named_aggregates = Vec::new();
    let mut named_list: Vec<types::Named> = Vec::new();
    let mut non_aggregates: Vec<types::Named> = Vec::new();
    let mut group_by_vars: HashSet<String> = HashSet::default();

    if let Some(group_by) = &query.group_by_exprs_opt {
        for (position, r) in group_by.exprs.iter().enumerate() {
            let e = parse_value_expression(&parsing_context, &r.column_expr)?;
            let named = match &*e {
                types::Expression::Variable(path_expr) => {
                    if let Some(alias) = &r.as_clause {
                        group_by_vars.insert(alias.clone());
                        types::Named::Expression(*e.clone(), Some(alias.clone()))
                    } else {
                        let l = path_expr.path_segments.last().unwrap();
                        match l {
                            PathSegment::AttrName(s) => {
                                group_by_vars.insert(s.clone());
                                types::Named::Expression(*e.clone(), Some(s.clone()))
                            }
                            PathSegment::ArrayIndex(_, _)
                            | PathSegment::Wildcard
                            | PathSegment::WildcardAttr => {
                                group_by_vars.insert(format!("_{}", position + 1));
                                types::Named::Expression(*e.clone(), Some(format!("_{}", position + 1)))
                            }
                        }
                    }
                }
                _ => {
                    if let Some(alias) = &r.as_clause {
                        group_by_vars.insert(alias.clone());
                        types::Named::Expression(*e.clone(), Some(alias.clone()))
                    } else {
                        group_by_vars.insert(format!("_{}", position + 1));
                        types::Named::Expression(*e.clone(), Some(format!("_{}", position + 1)))
                    }
                }
            };

            non_aggregates.push(named.clone());
            named_list.push(named);
        }
    }

    // Apply WHERE filter before SELECT projection (SQL evaluation order:
    // FROM → WHERE → SELECT). The filter needs access to all raw columns,
    // not just the projected ones.
    if let Some(where_expr) = query.where_expr_opt {
        let filter_formula = parse_logic(&parsing_context, &where_expr.expr)?;
        root = types::Node::Filter(filter_formula, Box::new(root));
    }

    match query.select_clause {
        ast::SelectClause::SelectExpressions(select_exprs) => {
            if !select_exprs.is_empty() {
                for (offset, select_expr) in select_exprs.iter().enumerate() {
                    if let Ok(mut named_aggregate) = parse_aggregate(&parsing_context, select_expr) {
                        match &named_aggregate.aggregate {
                            types::Aggregate::GroupAsAggregate(_) => {
                                unreachable!();
                            }
                            types::Aggregate::Avg(named) => {
                                if let types::Named::Star = named {
                                    return Err(ParseError::InvalidArguments("avg".to_string()));
                                }

                                named_aggregates.push(named_aggregate.clone());
                                named_list.push(named.clone());
                            }
                            types::Aggregate::Count(named) => {
                                named_aggregates.push(named_aggregate.clone());
                                named_list.push(named.clone());
                            }
                            types::Aggregate::First(named) => {
                                if let types::Named::Star = named {
                                    return Err(ParseError::InvalidArguments("first".to_string()));
                                }
                                named_aggregates.push(named_aggregate.clone());
                                named_list.push(named.clone());
                            }
                            types::Aggregate::Last(named) => {
                                if let types::Named::Star = named {
                                    return Err(ParseError::InvalidArguments("last".to_string()));
                                }
                                named_aggregates.push(named_aggregate.clone());
                                named_list.push(named.clone());
                            }
                            types::Aggregate::Sum(named) => {
                                let nnn = named.clone();
                                match &named {
                                    types::Named::Star => {
                                        return Err(ParseError::InvalidArguments("sum".to_string()));
                                    }
                                    types::Named::Expression(expr, opt_name) => match expr {
                                        types::Expression::Variable(path_expr) => {
                                            let l = path_expr.path_segments.last().unwrap();
                                            match l {
                                                PathSegment::AttrName(_s) => {
                                                    let p = PathExpr::new(vec![l.clone()]);
                                                    let n = types::Named::Expression(
                                                        types::Expression::Variable(p),
                                                        opt_name.clone(),
                                                    );
                                                    named_aggregate.aggregate = types::Aggregate::Sum(n);
                                                    named_aggregates.push(named_aggregate);
                                                    named_list.push(nnn);
                                                }
                                                PathSegment::ArrayIndex(_, _)
                                                | PathSegment::Wildcard
                                                | PathSegment::WildcardAttr => {
                                                    let s = format! {"_{}", offset};
                                                    let p = PathExpr::new(vec![PathSegment::AttrName(s)]);
                                                    let n = types::Named::Expression(
                                                        types::Expression::Variable(p),
                                                        opt_name.clone(),
                                                    );
                                                    named_aggregate.aggregate = types::Aggregate::Sum(n);
                                                    named_aggregates.push(named_aggregate);
                                                    named_list.push(nnn);
                                                }
                                            }
                                        }
                                        _ => {
                                            named_aggregates.push(named_aggregate.clone());
                                            named_list.push(named.clone());
                                        }
                                    },
                                }
                            }
                            types::Aggregate::Max(named) => {
                                if let types::Named::Star = named {
                                    return Err(ParseError::InvalidArguments("max".to_string()));
                                }
                                named_aggregates.push(named_aggregate.clone());
                                named_list.push(named.clone());
                            }
                            types::Aggregate::Min(named) => {
                                if let types::Named::Star = named {
                                    return Err(ParseError::InvalidArguments("min".to_string()));
                                }
                                named_aggregates.push(named_aggregate.clone());
                                named_list.push(named.clone());
                            }
                            types::Aggregate::ApproxCountDistinct(named) => {
                                if let types::Named::Star = named {
                                    return Err(ParseError::InvalidArguments("approx_count_distinct".to_string()));
                                }
                                named_aggregates.push(named_aggregate.clone());
                                named_list.push(named.clone());
                            }
                            types::Aggregate::PercentileDisc(_, column_name, _) => {
                                named_aggregates.push(named_aggregate.clone());
                                named_list.push(types::Named::Expression(
                                    types::Expression::Variable(column_name.clone()),
                                    Some(column_name.unwrap_last()),
                                ))
                            }
                            types::Aggregate::ApproxPercentile(_, column_name, _) => {
                                named_aggregates.push(named_aggregate.clone());
                                named_list.push(types::Named::Expression(
                                    types::Expression::Variable(column_name.clone()),
                                    Some(column_name.unwrap_last()),
                                ))
                            }
                        }
                    } else {
                        let named = *parse_expression(&parsing_context, select_expr)?;

                        if query.group_by_exprs_opt.is_some() {
                            if !check_group_by_vars(&named, &group_by_vars) {
                                return Err(ParseError::GroupByFieldsMismatch);
                            }
                        } else if let Some(group_as_clause) = query
                            .group_by_exprs_opt
                            .as_ref()
                            .and_then(|x| x.group_as_clause.as_ref())
                        {
                            if check_contains_group_as_var(&[named.clone()], group_as_clause) {
                                named_list.push(named.clone());
                                named_aggregates.push(types::NamedAggregate::new(
                                    types::Aggregate::GroupAsAggregate(named),
                                    Some(group_as_clause.clone()),
                                ));
                            }
                        } else {
                            non_aggregates.push(named.clone());
                            named_list.push(named.clone());
                        }
                    }
                }

                root = types::Node::Map(named_list.clone(), Box::new(root));
            }
        }
        ast::SelectClause::ValueConstructor(vc) => {
            match vc {
                ast::ValueConstructor::Expression(expr) => {
                    let e = parse_value_expression(&parsing_context, &expr)?;
                    let name = match &*e {
                        types::Expression::Variable(path_expr) => {
                            match path_expr.path_segments.last().unwrap() {
                                ast::PathSegment::AttrName(s) => Some(s.clone()),
                                _ => Some("_value".to_string()),
                            }
                        }
                        _ => Some("_value".to_string()),
                    };
                    named_list.push(types::Named::Expression(*e, name));
                    root = types::Node::Map(named_list.clone(), Box::new(root));
                }
                ast::ValueConstructor::TupleConstructor(tc) => {
                    for (key, val_expr) in tc.key_values.iter() {
                        let e = parse_value_expression(&parsing_context, val_expr)?;
                        named_list.push(types::Named::Expression(*e, Some(key.clone())));
                    }
                    root = types::Node::Map(named_list.clone(), Box::new(root));
                }
                ast::ValueConstructor::ArrayConstructor(ac) => {
                    for (idx, val_expr) in ac.values.iter().enumerate() {
                        let e = parse_value_expression(&parsing_context, val_expr)?;
                        let name = match &*e {
                            types::Expression::Variable(path_expr) => {
                                match path_expr.path_segments.last().unwrap() {
                                    ast::PathSegment::AttrName(s) => Some(s.clone()),
                                    _ => Some(format!("_{}", idx)),
                                }
                            }
                            _ => Some(format!("_{}", idx)),
                        };
                        named_list.push(types::Named::Expression(*e, name));
                    }
                    root = types::Node::Map(named_list.clone(), Box::new(root));
                }
            }
        }
    }

    if !named_aggregates.is_empty() {
        if let Some(group_by) = query.group_by_exprs_opt {
            let fields: Vec<PathExpr> = group_by
                .exprs
                .iter()
                .enumerate()
                .map(|(position, r)| {
                    let e = parse_value_expression(&parsing_context, &r.column_expr).unwrap();
                    match &*e {
                        types::Expression::Variable(path_expr) => {
                            if let Some(alias) = &r.as_clause {
                                PathExpr::new(vec![PathSegment::AttrName(alias.clone())])
                            } else {
                                let l = path_expr.path_segments.last().unwrap();
                                match l {
                                    PathSegment::AttrName(s) => PathExpr::new(vec![PathSegment::AttrName(s.clone())]),
                                    PathSegment::ArrayIndex(_, _) | PathSegment::Wildcard | PathSegment::WildcardAttr => {
                                        let s = format!("_{}", position + 1);
                                        PathExpr::new(vec![PathSegment::AttrName(s.clone())])
                                    }
                                }
                            }
                        }
                        _ => {
                            if let Some(alias) = &r.as_clause {
                                PathExpr::new(vec![PathSegment::AttrName(alias.clone())])
                            } else {
                                let s = format!("_{}", position + 1);
                                PathExpr::new(vec![PathSegment::AttrName(s.clone())])
                            }
                        }
                    }
                })
                .collect();

            // SELECT * with GROUP BY is not supported for jsonl or multi-table queries
            if non_aggregates.iter().any(|n| matches!(n, types::Named::Star)) {
                if data_sources.len() > 1 || file_format == "jsonl" {
                    return Err(ParseError::StarGroupByUnsupported);
                }
            }

            if !is_match_group_by_fields(&fields, &non_aggregates, &file_format) {
                return Err(ParseError::GroupByFieldsMismatch);
            }

            root = types::Node::GroupBy(fields, named_aggregates, Box::new(root));
        } else {
            let fields = Vec::new();
            root = types::Node::GroupBy(fields, named_aggregates, Box::new(root));
        }

        if let Some(having_expr) = query.having_expr_opt {
            let filter_formula = parse_logic(&parsing_context, &having_expr.expr)?;
            root = types::Node::Filter(filter_formula, Box::new(root));
        }
    } else {
        //sanity check if there is a group by statement
        if let Some(group_by_exprs) = query.group_by_exprs_opt {
            if let Some(group_as_clause) = group_by_exprs.group_as_clause {
                if !check_contains_group_as_var(&named_list, &group_as_clause) {
                    return Err(ParseError::GroupByWithoutAggregateFunction);
                }
            } else {
                return Err(ParseError::GroupByWithoutAggregateFunction);
            }
        }

        if query.having_expr_opt.is_some() {
            return Err(ParseError::HavingClauseWithoutGroupBy);
        }
    }

    if query.distinct {
        root = types::Node::Distinct(Box::new(root));
    }

    if let Some(order_by_expr) = query.order_by_expr_opt {
        let mut column_names = Vec::new();
        let mut orderings = Vec::new();
        for ordering_term in order_by_expr.ordering_terms {
            column_names.push(ordering_term.column_name.clone());
            let ordering = parse_ordering(ordering_term.ordering)?;
            orderings.push(ordering);
        }

        root = types::Node::OrderBy(column_names, orderings, Box::new(root));
    }

    if let Some(limit_expr) = query.limit_expr_opt {
        root = types::Node::Limit(limit_expr.row_count, Box::new(root));
    }

    Ok(root)
}

fn is_match_group_by_fields(variables: &[ast::PathExpr], named_list: &[types::Named], file_format: &str) -> bool {
    let a: HashSet<PathExpr> = variables.iter().map(|s| s.clone()).collect();
    let mut b: HashSet<PathExpr> = HashSet::new();

    for named in named_list.iter() {
        match named {
            types::Named::Expression(expr, name_opt) => {
                if let Some(name) = name_opt {
                    b.insert(PathExpr::new(vec![PathSegment::AttrName(name.clone())]));
                } else {
                    match expr {
                        types::Expression::Variable(path_expr) => {
                            b.insert(path_expr.clone());
                        }
                        _ => {
                            return false;
                        }
                    }
                }
            }
            types::Named::Star => {
                if file_format == "elb" {
                    for field_name in execution::datasource::ClassicLoadBalancerLogField::field_names().into_iter() {
                        b.insert(PathExpr::new(vec![PathSegment::AttrName(field_name.clone())]));
                    }
                } else if file_format == "alb" {
                    for field_name in execution::datasource::ApplicationLoadBalancerLogField::field_names().into_iter()
                    {
                        b.insert(PathExpr::new(vec![PathSegment::AttrName(field_name.clone())]));
                    }
                } else if file_format == "squid" {
                    for field_name in execution::datasource::SquidLogField::field_names().into_iter() {
                        b.insert(PathExpr::new(vec![PathSegment::AttrName(field_name.clone())]));
                    }
                } else if file_format == "s3" {
                    for field_name in execution::datasource::S3Field::field_names().into_iter() {
                        b.insert(PathExpr::new(vec![PathSegment::AttrName(field_name.clone())]));
                    }
                } else {
                    // jsonl has no fixed schema -- cannot expand Star
                    return false;
                }
            }
        }
    }

    a == b
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::syntax::ast::{FromClause, PathSegment, SelectClause};
    use std::sync::Arc;

    #[test]
    fn test_parse_logic_expression() {
        let before = ast::Expression::BinaryOperator(
            ast::BinaryOperator::And,
            Box::new(ast::Expression::Value(ast::Value::Boolean(true))),
            Box::new(ast::Expression::Value(ast::Value::Boolean(false))),
        );

        let parsing_context = ParsingContext {
            data_sources: vec![("a".to_string(), common::DataSource::Stdin("jsonl".to_string(), "a".to_string()))].into_iter().collect(),
            registry: Arc::new(crate::functions::register_all().unwrap()),
        };
        let expected = Box::new(types::Expression::Logic(Box::new(types::Formula::InfixOperator(
            types::LogicInfixOp::And,
            Box::new(types::Formula::Constant(true)),
            Box::new(types::Formula::Constant(false)),
        ))));

        let ans = parse_logic_expression(&parsing_context, &before).unwrap();
        assert_eq!(expected, ans);

        let before = ast::Expression::UnaryOperator(
            ast::UnaryOperator::Not,
            Box::new(ast::Expression::Value(ast::Value::Boolean(false))),
        );

        let expected = Box::new(types::Expression::Logic(Box::new(types::Formula::PrefixOperator(
            types::LogicPrefixOp::Not,
            Box::new(types::Formula::Constant(false)),
        ))));

        let parsing_context = ParsingContext {
            data_sources: vec![("a".to_string(), common::DataSource::Stdin("jsonl".to_string(), "a".to_string()))].into_iter().collect(),
            registry: Arc::new(crate::functions::register_all().unwrap()),
        };
        let ans = parse_logic_expression(&parsing_context, &before).unwrap();
        assert_eq!(expected, ans);
    }

    #[test]
    fn test_parse_value_expression() {
        let before = ast::Expression::BinaryOperator(
            ast::BinaryOperator::Plus,
            Box::new(ast::Expression::BinaryOperator(
                ast::BinaryOperator::Plus,
                Box::new(ast::Expression::Value(ast::Value::Integral(1))),
                Box::new(ast::Expression::Value(ast::Value::Integral(2))),
            )),
            Box::new(ast::Expression::Value(ast::Value::Integral(3))),
        );

        let expected = Box::new(types::Expression::Function(
            "Plus".to_string(),
            vec![
                types::Named::Expression(
                    types::Expression::Function(
                        "Plus".to_string(),
                        vec![
                            types::Named::Expression(types::Expression::Constant(common::Value::Int(1)), None),
                            types::Named::Expression(types::Expression::Constant(common::Value::Int(2)), None),
                        ],
                    ),
                    None,
                ),
                types::Named::Expression(types::Expression::Constant(common::Value::Int(3)), None),
            ],
        ));

        let parsing_context = ParsingContext {
            data_sources: vec![("a".to_string(), common::DataSource::Stdin("jsonl".to_string(), "a".to_string()))].into_iter().collect(),
            registry: Arc::new(crate::functions::register_all().unwrap()),
        };
        let ans = parse_value_expression(&parsing_context, &before).unwrap();
        assert_eq!(expected, ans);
    }

    #[test]
    fn test_parse_aggregate() {
        let path_expr_a = PathExpr::new(vec![PathSegment::AttrName("a".to_string())]);

        let before = ast::SelectExpression::Expression(
            Box::new(ast::Expression::FuncCall(
                "avg".to_string(),
                vec![ast::SelectExpression::Expression(
                    Box::new(ast::Expression::Column(path_expr_a.clone())),
                    None,
                )],
                None,
            )),
            None,
        );

        let named = types::Named::Expression(types::Expression::Variable(path_expr_a.clone()), Some("a".to_string()));
        let expected = types::NamedAggregate::new(types::Aggregate::Avg(named), None);

        let parsing_context = ParsingContext {
            data_sources: vec![("a".to_string(), common::DataSource::Stdin("jsonl".to_string(), "a".to_string()))].into_iter().collect(),
            registry: Arc::new(crate::functions::register_all().unwrap()),
        };
        let ans = parse_aggregate(&parsing_context, &before).unwrap();
        assert_eq!(expected, ans);
    }

    #[test]
    fn test_parse_condition() {
        let path_expr = PathExpr::new(vec![PathSegment::AttrName("a".to_string())]);
        let before = ast::Expression::BinaryOperator(
            ast::BinaryOperator::Equal,
            Box::new(ast::Expression::Column(path_expr.clone())),
            Box::new(ast::Expression::Value(ast::Value::Integral(1))),
        );

        let expected = Box::new(types::Formula::Predicate(
            types::Relation::Equal,
            Box::new(types::Expression::Variable(path_expr.clone())),
            Box::new(types::Expression::Constant(common::Value::Int(1))),
        ));

        let parsing_context = ParsingContext {
            data_sources: vec![("a".to_string(), common::DataSource::Stdin("jsonl".to_string(), "a".to_string()))].into_iter().collect(),
            registry: Arc::new(crate::functions::register_all().unwrap()),
        };
        let ans = parse_condition(&parsing_context, &before).unwrap();
        assert_eq!(expected, ans);
    }

    #[test]
    fn test_parse_query_with_simple_select_where_with_bindings() {
        let path_expr_a = PathExpr::new(vec![PathSegment::AttrName("a".to_string())]);
        let path_expr_b = PathExpr::new(vec![PathSegment::AttrName("b".to_string())]);

        let select_exprs = vec![
            ast::SelectExpression::Expression(Box::new(ast::Expression::Column(path_expr_a.clone())), None),
            ast::SelectExpression::Expression(Box::new(ast::Expression::Column(path_expr_b.clone())), None),
        ];

        let where_expr = ast::WhereExpression::new(ast::Expression::BinaryOperator(
            ast::BinaryOperator::Equal,
            Box::new(ast::Expression::Column(path_expr_a.clone())),
            Box::new(ast::Expression::Value(ast::Value::Integral(1))),
        ));

        let path_expr = PathExpr::new(vec![
            PathSegment::AttrName("it".to_string()),
            PathSegment::AttrName("a".to_string()),
        ]);

        let table_reference = ast::TableReference::new(path_expr, Some("e".to_string()), None);
        let before = ast::SelectStatement::new(
            false,
            SelectClause::SelectExpressions(select_exprs),
            FromClause::Tables(vec![table_reference]),
            Some(where_expr),
            None,
            None,
            None,
            None,
        );

        let filtered_formula = Box::new(types::Formula::Predicate(
            types::Relation::Equal,
            Box::new(types::Expression::Variable(path_expr_a.clone())),
            Box::new(types::Expression::Constant(common::Value::Int(1))),
        ));

        let path_expr = PathExpr::new(vec![PathSegment::AttrName("a".to_string())]);

        let bindings = vec![common::Binding {
            path_expr,
            name: "e".to_string(),
            idx_name: None,
        }];

        let expected = types::Node::Map(
            vec![
                types::Named::Expression(types::Expression::Variable(path_expr_a.clone()), Some("a".to_string())),
                types::Named::Expression(types::Expression::Variable(path_expr_b.clone()), Some("b".to_string())),
            ],
            Box::new(types::Node::Filter(
                filtered_formula,
                Box::new(types::Node::DataSource(
                    common::DataSource::Stdin("jsonl".to_string(), "it".to_string()),
                    bindings,
                )),
            )),
        );

        let registry = Arc::new(crate::functions::register_all().unwrap());
        let data_sources: common::DataSourceRegistry = vec![("it".to_string(), common::DataSource::Stdin("jsonl".to_string(), "it".to_string()))].into_iter().collect();
        let ans = parse_query(before, data_sources, registry).unwrap();
        assert_eq!(expected, ans);
    }

    #[test]
    fn test_parse_query_with_simple_select_where_no_from_clause_bindings() {
        let path_expr_a = PathExpr::new(vec![PathSegment::AttrName("a".to_string())]);
        let path_expr_b = PathExpr::new(vec![PathSegment::AttrName("b".to_string())]);

        let select_exprs = vec![
            ast::SelectExpression::Expression(Box::new(ast::Expression::Column(path_expr_a.clone())), None),
            ast::SelectExpression::Expression(Box::new(ast::Expression::Column(path_expr_b.clone())), None),
        ];

        let where_expr = ast::WhereExpression::new(ast::Expression::BinaryOperator(
            ast::BinaryOperator::Equal,
            Box::new(ast::Expression::Column(path_expr_a.clone())),
            Box::new(ast::Expression::Value(ast::Value::Integral(1))),
        ));

        let path_expr = PathExpr::new(vec![PathSegment::AttrName("it".to_string())]);
        let table_reference = ast::TableReference::new(path_expr, None, None);
        let before = ast::SelectStatement::new(
            false,
            SelectClause::SelectExpressions(select_exprs),
            FromClause::Tables(vec![table_reference]),
            Some(where_expr),
            None,
            None,
            None,
            None,
        );
        let filtered_formula = Box::new(types::Formula::Predicate(
            types::Relation::Equal,
            Box::new(types::Expression::Variable(path_expr_a.clone())),
            Box::new(types::Expression::Constant(common::Value::Int(1))),
        ));

        let expected = types::Node::Map(
            vec![
                types::Named::Expression(types::Expression::Variable(path_expr_a.clone()), Some("a".to_string())),
                types::Named::Expression(types::Expression::Variable(path_expr_b.clone()), Some("b".to_string())),
            ],
            Box::new(types::Node::Filter(
                filtered_formula,
                Box::new(types::Node::DataSource(
                    common::DataSource::Stdin("jsonl".to_string(), "it".to_string()),
                    vec![],
                )),
            )),
        );

        let registry = Arc::new(crate::functions::register_all().unwrap());
        let data_sources: common::DataSourceRegistry = vec![("it".to_string(), common::DataSource::Stdin("jsonl".to_string(), "it".to_string()))].into_iter().collect();
        let ans = parse_query(before, data_sources, registry).unwrap();
        assert_eq!(expected, ans);
    }

    #[test]
    fn test_parse_query_with_group_by() {
        let path_expr_a = PathExpr::new(vec![PathSegment::AttrName("a".to_string())]);
        let path_expr_b = PathExpr::new(vec![PathSegment::AttrName("b".to_string())]);
        let select_exprs = vec![
            ast::SelectExpression::Expression(
                Box::new(ast::Expression::FuncCall(
                    "avg".to_string(),
                    vec![ast::SelectExpression::Expression(
                        Box::new(ast::Expression::Column(path_expr_a.clone())),
                        None,
                    )],
                    None,
                )),
                None,
            ),
            ast::SelectExpression::Expression(Box::new(ast::Expression::Column(path_expr_b.clone())), None),
        ];

        let where_expr = ast::WhereExpression::new(ast::Expression::BinaryOperator(
            ast::BinaryOperator::Equal,
            Box::new(ast::Expression::Column(path_expr_a.clone())),
            Box::new(ast::Expression::Value(ast::Value::Integral(1))),
        ));
        let group_by_ref = ast::GroupByReference::new(ast::Expression::Column(path_expr_b.clone()), None);
        let group_by_expr = ast::GroupByExpression::new(vec![group_by_ref], None);
        let path_expr = PathExpr::new(vec![PathSegment::AttrName("it".to_string())]);
        let table_reference = ast::TableReference::new(path_expr, None, None);
        let before = ast::SelectStatement::new(
            false,
            SelectClause::SelectExpressions(select_exprs),
            FromClause::Tables(vec![table_reference]),
            Some(where_expr),
            Some(group_by_expr),
            None,
            None,
            None,
        );
        let filtered_formula = Box::new(types::Formula::Predicate(
            types::Relation::Equal,
            Box::new(types::Expression::Variable(path_expr_a.clone())),
            Box::new(types::Expression::Constant(common::Value::Int(1))),
        ));

        let path_expr = PathExpr::new(vec![PathSegment::AttrName("it".to_string())]);
        let _binding = common::Binding {
            path_expr,
            name: "e".to_string(),
            idx_name: None,
        };

        let filter = types::Node::Map(
            vec![
                types::Named::Expression(types::Expression::Variable(path_expr_b.clone()), Some("b".to_string())),
                types::Named::Expression(types::Expression::Variable(path_expr_a.clone()), Some("a".to_string())),
            ],
            Box::new(types::Node::Filter(
                filtered_formula,
                Box::new(types::Node::DataSource(
                    common::DataSource::Stdin("jsonl".to_string(), "it".to_string()),
                    vec![],
                )),
            )),
        );

        let named_aggregates = vec![types::NamedAggregate::new(
            types::Aggregate::Avg(types::Named::Expression(
                types::Expression::Variable(path_expr_a),
                Some("a".to_string()),
            )),
            None,
        )];

        let fields = vec![path_expr_b.clone()];
        let expected = types::Node::GroupBy(fields, named_aggregates, Box::new(filter));

        let registry = Arc::new(crate::functions::register_all().unwrap());
        let data_sources: common::DataSourceRegistry = vec![("it".to_string(), common::DataSource::Stdin("jsonl".to_string(), "it".to_string()))].into_iter().collect();
        let ans = parse_query(before, data_sources, registry).unwrap();
        assert_eq!(expected, ans);
    }

    #[test]
    fn test_parse_query_group_by_without_aggregate() {
        let path_expr_a = PathExpr::new(vec![PathSegment::AttrName("a".to_string())]);
        let path_expr_b = PathExpr::new(vec![PathSegment::AttrName("b".to_string())]);

        let select_exprs = vec![
            ast::SelectExpression::Expression(Box::new(ast::Expression::Column(path_expr_a.clone())), None),
            ast::SelectExpression::Expression(Box::new(ast::Expression::Column(path_expr_b.clone())), None),
        ];

        let where_expr = ast::WhereExpression::new(ast::Expression::BinaryOperator(
            ast::BinaryOperator::Equal,
            Box::new(ast::Expression::Column(path_expr_a.clone())),
            Box::new(ast::Expression::Value(ast::Value::Integral(1))),
        ));

        let group_by_ref = ast::GroupByReference::new(ast::Expression::Column(path_expr_b.clone()), None);
        let group_by_expr = ast::GroupByExpression::new(vec![group_by_ref], None);
        let path_expr = PathExpr::new(vec![PathSegment::AttrName("it".to_string())]);
        let table_reference = ast::TableReference::new(path_expr, None, None);
        let before = ast::SelectStatement::new(
            false,
            SelectClause::SelectExpressions(select_exprs),
            FromClause::Tables(vec![table_reference]),
            Some(where_expr),
            Some(group_by_expr),
            None,
            None,
            None,
        );
        let data_sources: common::DataSourceRegistry = vec![("it".to_string(), common::DataSource::Stdin("jsonl".to_string(), "it".to_string()))].into_iter().collect();
        let registry = Arc::new(crate::functions::register_all().unwrap());
        let ans = parse_query(before, data_sources, registry);
        let expected = Err(ParseError::GroupByFieldsMismatch);
        assert_eq!(expected, ans);
    }

    #[test]
    fn test_parse_query_group_by_mismatch_fields() {
        let path_expr_a = PathExpr::new(vec![PathSegment::AttrName("a".to_string())]);
        let path_expr_b = PathExpr::new(vec![PathSegment::AttrName("b".to_string())]);
        let path_expr_c = PathExpr::new(vec![PathSegment::AttrName("c".to_string())]);

        let select_exprs = vec![
            ast::SelectExpression::Expression(Box::new(ast::Expression::Column(path_expr_a.clone())), None),
            ast::SelectExpression::Expression(Box::new(ast::Expression::Column(path_expr_b.clone())), None),
            ast::SelectExpression::Expression(
                Box::new(ast::Expression::FuncCall(
                    "count".to_string(),
                    vec![ast::SelectExpression::Expression(
                        Box::new(ast::Expression::Column(path_expr_c.clone())),
                        None,
                    )],
                    None,
                )),
                None,
            ),
        ];

        let group_by_ref = ast::GroupByReference::new(ast::Expression::Column(path_expr_b.clone()), None);
        let group_by_expr = ast::GroupByExpression::new(vec![group_by_ref], None);
        let path_expr = PathExpr::new(vec![PathSegment::AttrName("it".to_string())]);
        let table_reference = ast::TableReference::new(path_expr, None, None);
        let before = ast::SelectStatement::new(
            false,
            SelectClause::SelectExpressions(select_exprs),
            FromClause::Tables(vec![table_reference]),
            None,
            Some(group_by_expr),
            None,
            None,
            None,
        );
        let data_sources: common::DataSourceRegistry = vec![("it".to_string(), common::DataSource::Stdin("jsonl".to_string(), "it".to_string()))].into_iter().collect();
        let registry = Arc::new(crate::functions::register_all().unwrap());
        let ans = parse_query(before, data_sources, registry);
        let expected = Err(ParseError::GroupByFieldsMismatch);
        assert_eq!(expected, ans);
    }

    #[test]
    fn test_parse_logic_func_call_in_boolean_context() {
        // WHERE upper(a) — a FuncCall used directly as the WHERE predicate
        let path_expr_a = PathExpr::new(vec![PathSegment::AttrName("a".to_string())]);
        let func_call = ast::Expression::FuncCall(
            "upper".to_string(),
            vec![ast::SelectExpression::Expression(
                Box::new(ast::Expression::Column(path_expr_a.clone())),
                None,
            )],
            None,
        );

        let parsing_context = ParsingContext {
            data_sources: vec![("it".to_string(), common::DataSource::Stdin("jsonl".to_string(), "it".to_string()))].into_iter().collect(),
            registry: Arc::new(crate::functions::register_all().unwrap()),
        };
        let result = parse_logic(&parsing_context, &func_call);
        assert!(result.is_ok());

        let formula = result.unwrap();
        match *formula {
            types::Formula::ExpressionPredicate(ref expr) => {
                match &**expr {
                    types::Expression::Function(name, _) => {
                        assert_eq!(name, "upper");
                    }
                    _ => panic!("Expected Function expression"),
                }
            }
            _ => panic!("Expected ExpressionPredicate formula"),
        }
    }

    #[test]
    fn test_parse_logic_column_in_boolean_context() {
        // WHERE active — a column reference used directly as boolean
        let path_expr = PathExpr::new(vec![PathSegment::AttrName("active".to_string())]);
        let column_expr = ast::Expression::Column(path_expr.clone());

        let parsing_context = ParsingContext {
            data_sources: vec![("it".to_string(), common::DataSource::Stdin("jsonl".to_string(), "it".to_string()))].into_iter().collect(),
            registry: Arc::new(crate::functions::register_all().unwrap()),
        };
        let result = parse_logic(&parsing_context, &column_expr);
        assert!(result.is_ok());

        let formula = result.unwrap();
        match *formula {
            types::Formula::ExpressionPredicate(ref expr) => {
                match &**expr {
                    types::Expression::Variable(p) => {
                        assert_eq!(p, &path_expr);
                    }
                    _ => panic!("Expected Variable expression"),
                }
            }
            _ => panic!("Expected ExpressionPredicate formula"),
        }
    }

    #[test]
    fn test_parse_logic_case_when_in_boolean_context() {
        // WHERE CASE WHEN a = 1 THEN true ELSE false END
        let path_expr_a = PathExpr::new(vec![PathSegment::AttrName("a".to_string())]);
        let case_when = ast::Expression::CaseWhenExpression(ast::CaseWhenExpression {
            branches: vec![(
                ast::Expression::BinaryOperator(
                    ast::BinaryOperator::Equal,
                    Box::new(ast::Expression::Column(path_expr_a.clone())),
                    Box::new(ast::Expression::Value(ast::Value::Integral(1))),
                ),
                ast::Expression::Value(ast::Value::Boolean(true)),
            )],
            else_expr: Some(Box::new(ast::Expression::Value(ast::Value::Boolean(false)))),
        });

        let parsing_context = ParsingContext {
            data_sources: vec![("it".to_string(), common::DataSource::Stdin("jsonl".to_string(), "it".to_string()))].into_iter().collect(),
            registry: Arc::new(crate::functions::register_all().unwrap()),
        };
        let result = parse_logic(&parsing_context, &case_when);
        assert!(result.is_ok());

        let formula = result.unwrap();
        match *formula {
            types::Formula::ExpressionPredicate(ref expr) => {
                match &**expr {
                    types::Expression::Branch(_, _) => {
                        // Successfully parsed as Branch expression
                    }
                    _ => panic!("Expected Branch expression"),
                }
            }
            _ => panic!("Expected ExpressionPredicate formula"),
        }
    }

    #[test]
    fn test_parse_condition_non_binary_expression() {
        // parse_condition with a non-BinaryOperator expression should produce ExpressionPredicate
        let path_expr_a = PathExpr::new(vec![PathSegment::AttrName("a".to_string())]);
        let column_expr = ast::Expression::Column(path_expr_a.clone());

        let parsing_context = ParsingContext {
            data_sources: vec![("it".to_string(), common::DataSource::Stdin("jsonl".to_string(), "it".to_string()))].into_iter().collect(),
            registry: Arc::new(crate::functions::register_all().unwrap()),
        };
        let result = parse_condition(&parsing_context, &column_expr);
        assert!(result.is_ok());

        let formula = result.unwrap();
        match *formula {
            types::Formula::ExpressionPredicate(ref expr) => {
                match &**expr {
                    types::Expression::Variable(p) => {
                        assert_eq!(p, &path_expr_a);
                    }
                    _ => panic!("Expected Variable expression"),
                }
            }
            _ => panic!("Expected ExpressionPredicate formula"),
        }
    }

    #[test]
    fn test_parse_query_select_value_expression() {
        // SELECT VALUE a FROM it WHERE a = 1
        let path_expr_a = PathExpr::new(vec![PathSegment::AttrName("a".to_string())]);

        let where_expr = ast::WhereExpression::new(ast::Expression::BinaryOperator(
            ast::BinaryOperator::Equal,
            Box::new(ast::Expression::Column(path_expr_a.clone())),
            Box::new(ast::Expression::Value(ast::Value::Integral(1))),
        ));

        let path_expr = PathExpr::new(vec![PathSegment::AttrName("it".to_string())]);
        let table_reference = ast::TableReference::new(path_expr, None, None);
        let before = ast::SelectStatement::new(
            false,
            SelectClause::ValueConstructor(ast::ValueConstructor::Expression(
                ast::Expression::Column(path_expr_a.clone()),
            )),
            FromClause::Tables(vec![table_reference]),
            Some(where_expr),
            None,
            None,
            None,
            None,
        );

        let filtered_formula = Box::new(types::Formula::Predicate(
            types::Relation::Equal,
            Box::new(types::Expression::Variable(path_expr_a.clone())),
            Box::new(types::Expression::Constant(common::Value::Int(1))),
        ));

        // SELECT VALUE a should produce Map(Filter(DataSource))
        let expected = types::Node::Map(
            vec![types::Named::Expression(
                types::Expression::Variable(path_expr_a.clone()),
                Some("a".to_string()),
            )],
            Box::new(types::Node::Filter(
                filtered_formula,
                Box::new(types::Node::DataSource(
                    common::DataSource::Stdin("jsonl".to_string(), "it".to_string()),
                    vec![],
                )),
            )),
        );

        let registry = Arc::new(crate::functions::register_all().unwrap());
        let data_sources: common::DataSourceRegistry = vec![("it".to_string(), common::DataSource::Stdin("jsonl".to_string(), "it".to_string()))].into_iter().collect();
        let ans = parse_query(before, data_sources, registry).unwrap();
        assert_eq!(expected, ans);
    }

    #[test]
    fn test_parse_query_select_value_literal() {
        // SELECT VALUE 42 FROM it
        let path_expr = PathExpr::new(vec![PathSegment::AttrName("it".to_string())]);
        let table_reference = ast::TableReference::new(path_expr, None, None);
        let before = ast::SelectStatement::new(
            false,
            SelectClause::ValueConstructor(ast::ValueConstructor::Expression(
                ast::Expression::Value(ast::Value::Integral(42)),
            )),
            FromClause::Tables(vec![table_reference]),
            None,
            None,
            None,
            None,
            None,
        );
        // Literal expression should get _value as the name
        let expected = types::Node::Map(
            vec![types::Named::Expression(
                types::Expression::Constant(common::Value::Int(42)),
                Some("_value".to_string()),
            )],
            Box::new(types::Node::DataSource(
                common::DataSource::Stdin("jsonl".to_string(), "it".to_string()),
                vec![],
            )),
        );

        let registry = Arc::new(crate::functions::register_all().unwrap());
        let data_sources: common::DataSourceRegistry = vec![("it".to_string(), common::DataSource::Stdin("jsonl".to_string(), "it".to_string()))].into_iter().collect();
        let ans = parse_query(before, data_sources, registry).unwrap();
        assert_eq!(expected, ans);
    }

    #[test]
    fn test_parse_query_cross_join_produces_cross_join_node() {
        let path_expr_ax = PathExpr::new(vec![
            PathSegment::AttrName("a".to_string()),
            PathSegment::AttrName("x".to_string()),
        ]);

        let select_exprs = vec![
            ast::SelectExpression::Expression(Box::new(ast::Expression::Column(path_expr_ax.clone())), None),
        ];

        let path_expr_it = PathExpr::new(vec![PathSegment::AttrName("it".to_string())]);
        let table_ref_a = ast::TableReference::new(path_expr_it.clone(), Some("a".to_string()), None);
        let table_ref_b = ast::TableReference::new(path_expr_it.clone(), Some("b".to_string()), None);

        let stmt = ast::SelectStatement::new(
            false,
            SelectClause::SelectExpressions(select_exprs),
            FromClause::Tables(vec![table_ref_a, table_ref_b]),
            None,
            None,
            None,
            None,
            None,
        );
        let data_source = common::DataSource::Stdin("jsonl".to_string(), "it".to_string());
        let data_sources: common::DataSourceRegistry = vec![("it".to_string(), data_source.clone())].into_iter().collect();
        let registry = Arc::new(crate::functions::register_all().unwrap());

        let result = parse_query(stmt, data_sources, registry);
        assert!(result.is_ok(), "Cross join query should parse: {:?}", result);

        let node = result.unwrap();
        // The root should be Map -> CrossJoin(DataSource, DataSource)
        match node {
            types::Node::Map(_, source) => match *source {
                types::Node::CrossJoin(left, right) => {
                    match *left {
                        types::Node::DataSource(ds, bindings) => {
                            assert_eq!(ds, data_source);
                            assert_eq!(bindings.len(), 1);
                            assert_eq!(bindings[0].name, "a");
                        }
                        other => panic!("Expected DataSource for left, got: {:?}", other),
                    }
                    match *right {
                        types::Node::DataSource(ds, bindings) => {
                            assert_eq!(ds, data_source);
                            assert_eq!(bindings.len(), 1);
                            assert_eq!(bindings[0].name, "b");
                        }
                        other => panic!("Expected DataSource for right, got: {:?}", other),
                    }
                }
                other => panic!("Expected CrossJoin node, got: {:?}", other),
            },
            other => panic!("Expected Map node at root, got: {:?}", other),
        }
    }

    #[test]
    fn test_parse_query_single_from_no_cross_join() {
        // A single table reference should NOT produce a CrossJoin node
        let path_expr_a = PathExpr::new(vec![PathSegment::AttrName("a".to_string())]);

        let select_exprs = vec![
            ast::SelectExpression::Expression(Box::new(ast::Expression::Column(path_expr_a.clone())), None),
        ];

        let path_expr_it = PathExpr::new(vec![PathSegment::AttrName("it".to_string())]);
        let table_ref = ast::TableReference::new(path_expr_it, None, None);

        let stmt = ast::SelectStatement::new(
            false,
            SelectClause::SelectExpressions(select_exprs),
            FromClause::Tables(vec![table_ref]),
            None,
            None,
            None,
            None,
            None,
        );
        let data_sources: common::DataSourceRegistry = vec![("it".to_string(), common::DataSource::Stdin("jsonl".to_string(), "it".to_string()))].into_iter().collect();
        let registry = Arc::new(crate::functions::register_all().unwrap());

        let result = parse_query(stmt, data_sources, registry);
        assert!(result.is_ok());

        let node = result.unwrap();
        match node {
            types::Node::Map(_, source) => match *source {
                types::Node::DataSource(_, _) => {
                    // Good - single table reference goes straight to DataSource
                }
                other => panic!("Expected DataSource node for single FROM, got: {:?}", other),
            },
            other => panic!("Expected Map node at root, got: {:?}", other),
        }
    }

    #[test]
    fn test_parse_query_left_join_produces_left_join_node() {
        let path_expr_ax = PathExpr::new(vec![
            PathSegment::AttrName("a".to_string()),
            PathSegment::AttrName("x".to_string()),
        ]);
        let path_expr_bx = PathExpr::new(vec![
            PathSegment::AttrName("b".to_string()),
            PathSegment::AttrName("x".to_string()),
        ]);

        let select_exprs = vec![
            ast::SelectExpression::Expression(Box::new(ast::Expression::Column(path_expr_ax.clone())), None),
            ast::SelectExpression::Expression(Box::new(ast::Expression::Column(path_expr_bx.clone())), None),
        ];

        let path_expr_it = PathExpr::new(vec![PathSegment::AttrName("it".to_string())]);
        let table_ref_a = ast::TableReference::new(path_expr_it.clone(), Some("a".to_string()), None);
        let table_ref_b = ast::TableReference::new(path_expr_it.clone(), Some("b".to_string()), None);

        let on_condition = ast::Expression::BinaryOperator(
            ast::BinaryOperator::Equal,
            Box::new(ast::Expression::Column(path_expr_ax.clone())),
            Box::new(ast::Expression::Column(path_expr_bx.clone())),
        );

        let from_clause = FromClause::Join {
            left: Box::new(FromClause::Tables(vec![table_ref_a])),
            right: table_ref_b,
            join_type: ast::JoinType::Left,
            condition: Some(on_condition),
        };

        let stmt = ast::SelectStatement::new(
            false,
            SelectClause::SelectExpressions(select_exprs),
            from_clause,
            None,
            None,
            None,
            None,
            None,
        );
        let data_source = common::DataSource::Stdin("jsonl".to_string(), "it".to_string());
        let data_sources: common::DataSourceRegistry = vec![("it".to_string(), data_source.clone())].into_iter().collect();
        let registry = Arc::new(crate::functions::register_all().unwrap());

        let result = parse_query(stmt, data_sources, registry);
        assert!(result.is_ok(), "Left join query should parse: {:?}", result);

        let node = result.unwrap();
        // The root should be Map -> LeftJoin(DataSource, DataSource, condition)
        match node {
            types::Node::Map(_, source) => match *source {
                types::Node::LeftJoin(left, right, _condition) => {
                    match *left {
                        types::Node::DataSource(ds, bindings) => {
                            assert_eq!(ds, data_source);
                            assert_eq!(bindings.len(), 1);
                            assert_eq!(bindings[0].name, "a");
                        }
                        other => panic!("Expected DataSource for left, got: {:?}", other),
                    }
                    match *right {
                        types::Node::DataSource(ds, bindings) => {
                            assert_eq!(ds, data_source);
                            assert_eq!(bindings.len(), 1);
                            assert_eq!(bindings[0].name, "b");
                        }
                        other => panic!("Expected DataSource for right, got: {:?}", other),
                    }
                }
                other => panic!("Expected LeftJoin node, got: {:?}", other),
            },
            other => panic!("Expected Map node at root, got: {:?}", other),
        }
    }
}
