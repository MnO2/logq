use super::types;
use crate::common::types as common;
use crate::common::types::ParsingContext;
use crate::execution;
use crate::logical::types::Named;
use crate::syntax::ast;
use crate::syntax::ast::{PathExpr, PathSegment, TableReference};
use hashbrown::HashSet;

#[derive(Fail, Debug, Clone, PartialEq, Eq)]
pub enum ParseError {
    #[fail(display = "Type Mismatch")]
    TypeMismatch,
    #[fail(display = "Not Aggregate Function")]
    NotAggregateFunction,
    #[fail(display = "Group By statement but no aggregate function provided")]
    GroupByWithoutAggregateFunction,
    #[fail(display = "Group By statement mismatch with the non-aggregate fields")]
    GroupByFieldsMismatch,
    #[fail(display = "Invalid Arguments: {}", _0)]
    InvalidArguments(String),
    #[fail(display = "Invalid Arguments: {}", _0)]
    UnknownFunction(String),
    #[fail(display = "Having clause but no Group By clause provided")]
    HavingClauseWithoutGroupBy,
    #[fail(display = "Invalid table reference in From Clause")]
    FromClausePathInvalidTableReference,
    #[fail(display = "Using 'as' to define an alias is required in From Clause for nested path expr")]
    FromClauseMissingAsForPathExpr,
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
        _ => unreachable!(),
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
    }
}

fn parse_case_when_expression(
    ctx: &common::ParsingContext,
    value_expr: &ast::Expression,
) -> ParseResult<Box<types::Expression>> {
    match value_expr {
        ast::Expression::CaseWhenExpression(case_when_expr) => {
            let branch = parse_logic(ctx, &case_when_expr.condition)?;
            let then_expr = parse_value_expression(ctx, &case_when_expr.then_expr)?;
            let else_expr = if let Some(e) = &case_when_expr.else_expr {
                Some(parse_value_expression(ctx, &e)?)
            } else {
                None
            };

            Ok(Box::new(types::Expression::Branch(branch, then_expr, else_expr)))
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
            let mut args = Vec::new();
            for select_expr in select_exprs.iter() {
                let arg = parse_expression(ctx, select_expr)?;
                args.push(*arg);
            }
            Ok(Box::new(types::Expression::Function(func_name.clone(), args)))
        }
        ast::Expression::CaseWhenExpression(_) => parse_case_when_expression(ctx, value_expr),
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
        _ => unreachable!(),
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

fn check_env(table_name: &String, table_references: &Vec<TableReference>) -> ParseResult<()> {
    for (i, table_reference) in table_references.iter().enumerate() {
        if i == 0 {
            match &table_reference.path_expr.path_segments[0] {
                PathSegment::AttrName(s) => {
                    if !s.eq(table_name) {
                        return Err(ParseError::FromClausePathInvalidTableReference);
                    }
                }
                _ => return Err(ParseError::FromClausePathInvalidTableReference),
            }
        }
        let path_expr = &table_reference.path_expr;

        if path_expr.path_segments.is_empty() {
            return Err(ParseError::FromClausePathInvalidTableReference);
        }

        if path_expr.path_segments.len() > 1 && table_reference.as_clause.is_none() {
            return Err(ParseError::FromClauseMissingAsForPathExpr);
        }
    }

    Ok(())
}

fn to_bindings(table_name: &String, table_references: &Vec<TableReference>) -> Vec<common::Binding> {
    table_references
        .iter()
        .map(|table_reference| {
            let path_expr = match &table_reference.path_expr.path_segments[0] {
                PathSegment::AttrName(s) => {
                    if s.eq(table_name) {
                        PathExpr::new(
                            table_reference
                                .path_expr
                                .path_segments
                                .iter()
                                .skip(1)
                                .cloned()
                                .collect(),
                        )
                    } else {
                        table_reference.path_expr.clone()
                    }
                }
                _ => table_reference.path_expr.clone(),
            };

            if let Some(name) = table_reference.as_clause.clone() {
                Some(common::Binding {
                    path_expr,
                    name,
                    idx_name: table_reference.at_clause.clone(),
                })
            } else {
                None
            }
        })
        .filter_map(|x| x)
        .collect()
}

fn check_group_by_vars(named: &Named, group_by_vars: &HashSet<String>) -> bool {
    println!("named = {:?}, group_by_vars: {:?}", named, group_by_vars);
    match named {
        Named::Expression(expr, alias) => {
            match expr {
                types::Expression::Variable(path_expr) => match path_expr.path_segments.last().unwrap() {
                    PathSegment::AttrName(s) => {
                        return group_by_vars.contains(s);
                    }
                    PathSegment::ArrayIndex(_, _) => {
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

pub(crate) fn parse_query(query: ast::SelectStatement, data_source: common::DataSource) -> ParseResult<types::Node> {
    let table_references = &query.table_references;

    let (file_format, table_name) = match &data_source {
        common::DataSource::File(_, file_format, table_name) => (file_format.clone(), table_name.clone()),
        common::DataSource::Stdin(file_format, table_name) => (file_format.clone(), table_name.clone()),
    };

    check_env(&table_name, table_references)?;

    let parsing_context = ParsingContext {
        table_name: table_name.clone(),
    };
    let bindings = to_bindings(&table_name, table_references);

    let mut root = types::Node::DataSource(data_source, bindings);
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
                            PathSegment::ArrayIndex(_s, _idx) => {
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

    match query.select_clause {
        ast::SelectClause::SelectExpressions(select_exprs) => {
            if !select_exprs.is_empty() {
                for select_expr in select_exprs.iter() {
                    if let Ok(named_aggregate) = parse_aggregate(&parsing_context, select_expr) {
                        named_aggregates.push(named_aggregate.clone());

                        match named_aggregate.aggregate {
                            types::Aggregate::GroupAsAggregate(_) => {
                                unreachable!();
                            }
                            types::Aggregate::Avg(named) => {
                                if let types::Named::Star = named {
                                    return Err(ParseError::InvalidArguments("avg".to_string()));
                                }

                                named_list.push(named);
                            }
                            types::Aggregate::Count(named) => {
                                named_list.push(named);
                            }
                            types::Aggregate::First(named) => {
                                if let types::Named::Star = named {
                                    return Err(ParseError::InvalidArguments("first".to_string()));
                                }
                                named_list.push(named);
                            }
                            types::Aggregate::Last(named) => {
                                if let types::Named::Star = named {
                                    return Err(ParseError::InvalidArguments("last".to_string()));
                                }
                                named_list.push(named);
                            }
                            types::Aggregate::Sum(named) => {
                                if let types::Named::Star = named {
                                    return Err(ParseError::InvalidArguments("sum".to_string()));
                                }
                                named_list.push(named);
                            }
                            types::Aggregate::Max(named) => {
                                if let types::Named::Star = named {
                                    return Err(ParseError::InvalidArguments("max".to_string()));
                                }
                                named_list.push(named);
                            }
                            types::Aggregate::Min(named) => {
                                if let types::Named::Star = named {
                                    return Err(ParseError::InvalidArguments("min".to_string()));
                                }
                                named_list.push(named);
                            }
                            types::Aggregate::ApproxCountDistinct(named) => {
                                if let types::Named::Star = named {
                                    return Err(ParseError::InvalidArguments("approx_count_distinct".to_string()));
                                }
                                named_list.push(named);
                            }
                            types::Aggregate::PercentileDisc(_, column_name, _) => {
                                named_list.push(types::Named::Expression(
                                    types::Expression::Variable(column_name.clone()),
                                    Some(column_name.unwrap_last()),
                                ))
                            }
                            types::Aggregate::ApproxPercentile(_, column_name, _) => {
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
        ast::SelectClause::ValueConstructor(_vc) => {
            unimplemented!()
        }
    }

    if let Some(where_expr) = query.where_expr_opt {
        let filter_formula = parse_logic(&parsing_context, &where_expr.expr)?;
        root = types::Node::Filter(filter_formula, Box::new(root));
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
                                    PathSegment::ArrayIndex(_s, _idx) => {
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

            if !is_match_group_by_fields(&fields, &non_aggregates, &file_format) {
                return Err(ParseError::GroupByFieldsMismatch);
            }

            root = types::Node::GroupBy(
                fields,
                named_aggregates,
                group_by.group_as_clause.clone(),
                Box::new(root),
            );
        } else {
            let fields = Vec::new();
            root = types::Node::GroupBy(fields, named_aggregates, None, Box::new(root));
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

    println!("{:?}", &root);
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
                    unreachable!();
                }
            }
        }
    }

    println!("a = {:?}, b = {:?}", &a, &b);
    if a.len() != b.len() {
        false
    } else {
        let mut a_iter = a.iter();
        let mut b_iter = b.iter();
        while let (Some(aa), Some(bb)) = (a_iter.next(), b_iter.next()) {
            if aa != bb {
                return false;
            }
        }

        true
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::syntax::ast::{PathSegment, SelectClause};

    #[test]
    fn test_parse_logic_expression() {
        let before = ast::Expression::BinaryOperator(
            ast::BinaryOperator::And,
            Box::new(ast::Expression::Value(ast::Value::Boolean(true))),
            Box::new(ast::Expression::Value(ast::Value::Boolean(false))),
        );

        let parsing_context = ParsingContext {
            table_name: "a".to_string(),
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
            table_name: "a".to_string(),
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
            table_name: "a".to_string(),
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
            table_name: "a".to_string(),
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
            table_name: "a".to_string(),
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
            SelectClause::SelectExpressions(select_exprs),
            vec![table_reference],
            Some(where_expr),
            None,
            None,
            None,
            None,
        );
        let data_source = common::DataSource::Stdin("jsonl".to_string(), "it".to_string());

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

        let expected = types::Node::Filter(
            filtered_formula,
            Box::new(types::Node::Map(
                vec![
                    types::Named::Expression(types::Expression::Variable(path_expr_a.clone()), Some("a".to_string())),
                    types::Named::Expression(types::Expression::Variable(path_expr_b.clone()), Some("b".to_string())),
                ],
                Box::new(types::Node::DataSource(
                    common::DataSource::Stdin("jsonl".to_string(), "it".to_string()),
                    bindings,
                )),
            )),
        );

        let ans = parse_query(before, data_source).unwrap();
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
            SelectClause::SelectExpressions(select_exprs),
            vec![table_reference],
            Some(where_expr),
            None,
            None,
            None,
            None,
        );
        let data_source = common::DataSource::Stdin("jsonl".to_string(), "it".to_string());

        let filtered_formula = Box::new(types::Formula::Predicate(
            types::Relation::Equal,
            Box::new(types::Expression::Variable(path_expr_a.clone())),
            Box::new(types::Expression::Constant(common::Value::Int(1))),
        ));

        let expected = types::Node::Filter(
            filtered_formula,
            Box::new(types::Node::Map(
                vec![
                    types::Named::Expression(types::Expression::Variable(path_expr_a.clone()), Some("a".to_string())),
                    types::Named::Expression(types::Expression::Variable(path_expr_b.clone()), Some("b".to_string())),
                ],
                Box::new(types::Node::DataSource(
                    common::DataSource::Stdin("jsonl".to_string(), "it".to_string()),
                    vec![],
                )),
            )),
        );

        let ans = parse_query(before, data_source).unwrap();
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
            SelectClause::SelectExpressions(select_exprs),
            vec![table_reference],
            Some(where_expr),
            Some(group_by_expr),
            None,
            None,
            None,
        );
        let data_source = common::DataSource::Stdin("jsonl".to_string(), "it".to_string());

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

        let filter = types::Node::Filter(
            filtered_formula,
            Box::new(types::Node::Map(
                vec![
                    types::Named::Expression(types::Expression::Variable(path_expr_b.clone()), Some("b".to_string())),
                    types::Named::Expression(types::Expression::Variable(path_expr_a.clone()), Some("a".to_string())),
                ],
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
        let expected = types::Node::GroupBy(fields, named_aggregates, None, Box::new(filter));

        let ans = parse_query(before, data_source).unwrap();
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
            SelectClause::SelectExpressions(select_exprs),
            vec![table_reference],
            Some(where_expr),
            Some(group_by_expr),
            None,
            None,
            None,
        );
        let data_source = common::DataSource::Stdin("jsonl".to_string(), "it".to_string());
        let ans = parse_query(before, data_source);
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
            SelectClause::SelectExpressions(select_exprs),
            vec![table_reference],
            None,
            Some(group_by_expr),
            None,
            None,
            None,
        );
        let data_source = common::DataSource::Stdin("jsonl".to_string(), "it".to_string());
        let ans = parse_query(before, data_source);
        let expected = Err(ParseError::GroupByFieldsMismatch);
        assert_eq!(expected, ans);
    }
}
