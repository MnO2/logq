use super::types;
use crate::common::types as common;
use crate::common::types::VariableName;
use crate::execution;
use crate::syntax::ast;
use crate::syntax::ast::{PathExpr, PathSegment, TableReference};
use std::collections::hash_set::HashSet;

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
    #[fail(display = "Conflict variable naming")]
    ConflictVariableNaming,
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

fn parse_prefix_operator(op: types::LogicPrefixOp, child: &ast::Expression) -> ParseResult<Box<types::Formula>> {
    let child_parsed = parse_logic(child)?;

    let prefix_op = types::Formula::PrefixOperator(op, child_parsed);
    Ok(Box::new(prefix_op))
}

fn parse_infix_operator(
    op: types::LogicInfixOp,
    left: &ast::Expression,
    right: &ast::Expression,
) -> ParseResult<Box<types::Formula>> {
    let left_parsed = parse_logic(left)?;
    let right_parsed = parse_logic(right)?;

    let infix_op = types::Formula::InfixOperator(op, left_parsed, right_parsed);
    Ok(Box::new(infix_op))
}

fn parse_logic(expr: &ast::Expression) -> ParseResult<Box<types::Formula>> {
    match expr {
        ast::Expression::BinaryOperator(op, l, r) => {
            if op == &ast::BinaryOperator::And {
                let formula = parse_infix_operator(types::LogicInfixOp::And, l, r)?;
                Ok(formula)
            } else if op == &ast::BinaryOperator::Or {
                let formula = parse_infix_operator(types::LogicInfixOp::Or, l, r)?;
                Ok(formula)
            } else {
                parse_condition(expr)
            }
        }
        ast::Expression::UnaryOperator(op, c) => {
            if op == &ast::UnaryOperator::Not {
                let formula = parse_prefix_operator(types::LogicPrefixOp::Not, c)?;
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

fn parse_logic_expression(expr: &ast::Expression) -> ParseResult<Box<types::Expression>> {
    let formula = parse_logic(expr)?;
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

fn parse_case_when_expression(value_expr: &ast::Expression) -> ParseResult<Box<types::Expression>> {
    match value_expr {
        ast::Expression::CaseWhenExpression(case_when_expr) => {
            let branch = parse_logic(&case_when_expr.condition)?;
            let then_expr = parse_value_expression(&case_when_expr.then_expr)?;
            let else_expr = if let Some(e) = &case_when_expr.else_expr {
                Some(parse_value_expression(&e)?)
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

fn parse_binary_operator(value_expr: &ast::Expression) -> ParseResult<Box<types::Expression>> {
    match value_expr {
        ast::Expression::BinaryOperator(op, left_expr, right_expr) => {
            if op == &ast::BinaryOperator::And || op == &ast::BinaryOperator::Or {
                parse_logic_expression(value_expr)
            } else if op == &ast::BinaryOperator::Equal
                || op == &ast::BinaryOperator::NotEqual
                || op == &ast::BinaryOperator::MoreThan
                || op == &ast::BinaryOperator::LessThan
                || op == &ast::BinaryOperator::GreaterEqual
                || op == &ast::BinaryOperator::LessEqual
            {
                let formula = parse_condition(value_expr)?;
                Ok(Box::new(types::Expression::Logic(formula)))
            } else {
                let func_name = (*op).to_string();
                let left = parse_value_expression(left_expr)?;
                let right = parse_value_expression(right_expr)?;
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

fn parse_unary_operator(value_expr: &ast::Expression) -> ParseResult<Box<types::Expression>> {
    match value_expr {
        ast::Expression::UnaryOperator(op, expr) => {
            if op == &ast::UnaryOperator::Not {
                let formula = parse_prefix_operator(types::LogicPrefixOp::Not, expr)?;
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

fn parse_value_expression(value_expr: &ast::Expression) -> ParseResult<Box<types::Expression>> {
    match value_expr {
        ast::Expression::Value(v) => {
            let expr = parse_value(v)?;
            Ok(expr)
        }
        ast::Expression::Column(column_name) => Ok(Box::new(types::Expression::Variable(column_name.clone()))),
        ast::Expression::BinaryOperator(_, _, _) => parse_binary_operator(value_expr),
        ast::Expression::UnaryOperator(_, _) => parse_unary_operator(value_expr),
        ast::Expression::FuncCall(func_name, select_exprs, _) => {
            let mut args = Vec::new();
            for select_expr in select_exprs.iter() {
                let arg = parse_expression(select_expr)?;
                args.push(*arg);
            }
            Ok(Box::new(types::Expression::Function(func_name.clone(), args)))
        }
        ast::Expression::CaseWhenExpression(_) => parse_case_when_expression(value_expr),
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

fn parse_condition(condition: &ast::Expression) -> ParseResult<Box<types::Formula>> {
    match condition {
        ast::Expression::BinaryOperator(op, left_expr, right_expr) => {
            let left = parse_value_expression(left_expr)?;
            let right = parse_value_expression(right_expr)?;
            let rel_op = parse_relation(op)?;
            Ok(Box::new(types::Formula::Predicate(rel_op, left, right)))
        }
        _ => unreachable!(),
    }
}

fn parse_expression(select_expr: &ast::SelectExpression) -> ParseResult<Box<types::Named>> {
    match select_expr {
        ast::SelectExpression::Star => Ok(Box::new(types::Named::Star)),
        ast::SelectExpression::Expression(expr, name_opt) => {
            let e = parse_value_expression(expr)?;
            match &*e {
                types::Expression::Variable(name) => {
                    if let Some(rename) = name_opt {
                        Ok(Box::new(types::Named::Expression(*e.clone(), Some(rename.clone()))))
                    } else {
                        Ok(Box::new(types::Named::Expression(*e.clone(), Some(name.clone()))))
                    }
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

fn parse_aggregate(select_expr: &ast::SelectExpression) -> ParseResult<types::NamedAggregate> {
    match select_expr {
        ast::SelectExpression::Expression(expr, name_opt) => match &**expr {
            ast::Expression::FuncCall(func_name, args, within_group_opt) => {
                let named = *parse_expression(&args[0])?;

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

fn check_conflict_naming(named_list: &[types::Named]) -> bool {
    let mut name_set: HashSet<String> = HashSet::new();
    for named in named_list {
        match named {
            types::Named::Expression(expr, var_name_opt) => {
                if let Some(var_name) = var_name_opt {
                    if name_set.contains(var_name) {
                        return true;
                    } else {
                        name_set.insert(var_name.clone());
                    }
                } else if let types::Expression::Variable(var_name) = expr {
                    if name_set.contains(var_name) {
                        return true;
                    }

                    name_set.insert(var_name.clone());
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
                Some(common::Binding { path_expr, name })
            } else {
                None
            }
        })
        .filter_map(|x| x)
        .collect()
}

pub(crate) fn parse_query(query: ast::SelectStatement, data_source: common::DataSource) -> ParseResult<types::Node> {
    let table_references = &query.table_references;

    let (file_format, table_name) = match &data_source {
        common::DataSource::File(_, file_format, table_name) => (file_format.clone(), table_name.clone()),
        common::DataSource::Stdin(file_format, table_name) => (file_format.clone(), table_name.clone()),
    };

    check_env(&table_name, table_references)?;

    let bindings = to_bindings(&table_name, table_references);

    let mut root = types::Node::DataSource(data_source, bindings);
    let mut named_aggregates = Vec::new();
    let mut named_list: Vec<types::Named> = Vec::new();
    let mut non_aggregates: Vec<types::Named> = Vec::new();

    if !query.select_exprs.is_empty() {
        for select_expr in query.select_exprs.iter() {
            if let Ok(named_aggregate) = parse_aggregate(select_expr) {
                named_aggregates.push(named_aggregate.clone());

                match named_aggregate.aggregate {
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
                    types::Aggregate::PercentileDisc(_, column_name, _) => named_list.push(types::Named::Expression(
                        types::Expression::Variable(column_name.clone()),
                        Some(column_name.clone()),
                    )),
                    types::Aggregate::ApproxPercentile(_, column_name, _) => named_list.push(types::Named::Expression(
                        types::Expression::Variable(column_name.clone()),
                        Some(column_name.clone()),
                    )),
                }
            } else {
                let named = *parse_expression(select_expr)?;
                non_aggregates.push(named.clone());
                named_list.push(named);
            }
        }

        if check_conflict_naming(&named_list) {
            return Err(ParseError::ConflictVariableNaming);
        }

        root = types::Node::Map(named_list, Box::new(root));
    }

    if let Some(where_expr) = query.where_expr_opt {
        let filter_formula = parse_logic(&where_expr.expr)?;
        root = types::Node::Filter(filter_formula, Box::new(root));
    }

    if !named_aggregates.is_empty() {
        if let Some(group_by) = query.group_by_exprs_opt {
            let fields: Vec<VariableName> = group_by
                .exprs
                .iter()
                .map(|r| r.column_name.last().unwrap().clone())
                .collect();

            if !is_match_group_by_fields(&fields, &non_aggregates, &file_format) {
                return Err(ParseError::GroupByFieldsMismatch);
            }

            root = types::Node::GroupBy(fields, named_aggregates, Box::new(root));

            if let Some(having_expr) = query.having_expr_opt {
                let filter_formula = parse_logic(&having_expr.expr)?;
                root = types::Node::Filter(filter_formula, Box::new(root));
            }
        } else {
            let fields = Vec::new();
            root = types::Node::GroupBy(fields, named_aggregates, Box::new(root));

            if let Some(having_expr) = query.having_expr_opt {
                let filter_formula = parse_logic(&having_expr.expr)?;
                root = types::Node::Filter(filter_formula, Box::new(root));
            }
        }
    } else {
        //sanity check if there is a group by statement
        if query.group_by_exprs_opt.is_some() {
            return Err(ParseError::GroupByWithoutAggregateFunction);
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

    Ok(root)
}

fn is_match_group_by_fields(
    variables: &[common::VariableName],
    named_list: &[types::Named],
    file_format: &str,
) -> bool {
    let mut a: Vec<String> = variables.to_vec();
    let mut b: Vec<String> = Vec::new();

    for named in named_list.iter() {
        match named {
            types::Named::Expression(expr, name_opt) => {
                if let Some(name) = name_opt {
                    b.push(name.clone());
                } else {
                    match expr {
                        types::Expression::Variable(var_name) => {
                            b.push(var_name.clone());
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
                        b.push(field_name.clone());
                    }
                } else if file_format == "alb" {
                    for field_name in execution::datasource::ApplicationLoadBalancerLogField::field_names().into_iter()
                    {
                        b.push(field_name.clone());
                    }
                } else if file_format == "squid" {
                    for field_name in execution::datasource::SquidLogField::field_names().into_iter() {
                        b.push(field_name.clone());
                    }
                } else if file_format == "s3" {
                    for field_name in execution::datasource::S3Field::field_names().into_iter() {
                        b.push(field_name.clone());
                    }
                } else {
                    unreachable!();
                }
            }
        }
    }

    if a.len() != b.len() {
        false
    } else {
        a.sort();
        b.sort();

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
    use crate::syntax::ast::PathSegment;

    #[test]
    fn test_parse_logic_expression() {
        let before = ast::Expression::BinaryOperator(
            ast::BinaryOperator::And,
            Box::new(ast::Expression::Value(ast::Value::Boolean(true))),
            Box::new(ast::Expression::Value(ast::Value::Boolean(false))),
        );

        let expected = Box::new(types::Expression::Logic(Box::new(types::Formula::InfixOperator(
            types::LogicInfixOp::And,
            Box::new(types::Formula::Constant(true)),
            Box::new(types::Formula::Constant(false)),
        ))));

        let ans = parse_logic_expression(&before).unwrap();
        assert_eq!(expected, ans);

        let before = ast::Expression::UnaryOperator(
            ast::UnaryOperator::Not,
            Box::new(ast::Expression::Value(ast::Value::Boolean(false))),
        );

        let expected = Box::new(types::Expression::Logic(Box::new(types::Formula::PrefixOperator(
            types::LogicPrefixOp::Not,
            Box::new(types::Formula::Constant(false)),
        ))));

        let ans = parse_logic_expression(&before).unwrap();
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

        let ans = parse_value_expression(&before).unwrap();
        assert_eq!(expected, ans);
    }

    #[test]
    fn test_parse_aggregate() {
        let before = ast::SelectExpression::Expression(
            Box::new(ast::Expression::FuncCall(
                "avg".to_string(),
                vec![ast::SelectExpression::Expression(
                    Box::new(ast::Expression::Column("a".to_string())),
                    None,
                )],
                None,
            )),
            None,
        );

        let named = types::Named::Expression(types::Expression::Variable("a".to_string()), Some("a".to_string()));
        let expected = types::NamedAggregate::new(types::Aggregate::Avg(named), None);

        let ans = parse_aggregate(&before).unwrap();
        assert_eq!(expected, ans);
    }

    #[test]
    fn test_parse_condition() {
        let before = ast::Expression::BinaryOperator(
            ast::BinaryOperator::Equal,
            Box::new(ast::Expression::Column("a".to_string())),
            Box::new(ast::Expression::Value(ast::Value::Integral(1))),
        );

        let expected = Box::new(types::Formula::Predicate(
            types::Relation::Equal,
            Box::new(types::Expression::Variable("a".to_string())),
            Box::new(types::Expression::Constant(common::Value::Int(1))),
        ));

        let ans = parse_condition(&before).unwrap();
        assert_eq!(expected, ans);
    }

    #[test]
    fn test_parse_query_with_simple_select_where_with_bindings() {
        let select_exprs = vec![
            ast::SelectExpression::Expression(Box::new(ast::Expression::Column("a".to_string())), None),
            ast::SelectExpression::Expression(Box::new(ast::Expression::Column("b".to_string())), None),
        ];

        let where_expr = ast::WhereExpression::new(ast::Expression::BinaryOperator(
            ast::BinaryOperator::Equal,
            Box::new(ast::Expression::Column("a".to_string())),
            Box::new(ast::Expression::Value(ast::Value::Integral(1))),
        ));

        let path_expr = PathExpr::new(vec![
            PathSegment::AttrName("it".to_string()),
            PathSegment::AttrName("a".to_string()),
        ]);

        let table_reference = ast::TableReference::new(path_expr, Some("e".to_string()), None);
        let before = ast::SelectStatement::new(
            select_exprs,
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
            Box::new(types::Expression::Variable("a".to_string())),
            Box::new(types::Expression::Constant(common::Value::Int(1))),
        ));

        let path_expr = PathExpr::new(vec![PathSegment::AttrName("a".to_string())]);

        let bindings = vec![common::Binding {
            path_expr,
            name: "e".to_string(),
        }];

        let expected = types::Node::Filter(
            filtered_formula,
            Box::new(types::Node::Map(
                vec![
                    types::Named::Expression(types::Expression::Variable("a".to_string()), Some("a".to_string())),
                    types::Named::Expression(types::Expression::Variable("b".to_string()), Some("b".to_string())),
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
        let select_exprs = vec![
            ast::SelectExpression::Expression(Box::new(ast::Expression::Column("a".to_string())), None),
            ast::SelectExpression::Expression(Box::new(ast::Expression::Column("b".to_string())), None),
        ];

        let where_expr = ast::WhereExpression::new(ast::Expression::BinaryOperator(
            ast::BinaryOperator::Equal,
            Box::new(ast::Expression::Column("a".to_string())),
            Box::new(ast::Expression::Value(ast::Value::Integral(1))),
        ));

        let path_expr = PathExpr::new(vec![PathSegment::AttrName("it".to_string())]);
        let table_reference = ast::TableReference::new(path_expr, None, None);
        let before = ast::SelectStatement::new(
            select_exprs,
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
            Box::new(types::Expression::Variable("a".to_string())),
            Box::new(types::Expression::Constant(common::Value::Int(1))),
        ));

        let expected = types::Node::Filter(
            filtered_formula,
            Box::new(types::Node::Map(
                vec![
                    types::Named::Expression(types::Expression::Variable("a".to_string()), Some("a".to_string())),
                    types::Named::Expression(types::Expression::Variable("b".to_string()), Some("b".to_string())),
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
        let select_exprs = vec![
            ast::SelectExpression::Expression(
                Box::new(ast::Expression::FuncCall(
                    "avg".to_string(),
                    vec![ast::SelectExpression::Expression(
                        Box::new(ast::Expression::Column("a".to_string())),
                        None,
                    )],
                    None,
                )),
                None,
            ),
            ast::SelectExpression::Expression(Box::new(ast::Expression::Column("b".to_string())), None),
        ];

        let where_expr = ast::WhereExpression::new(ast::Expression::BinaryOperator(
            ast::BinaryOperator::Equal,
            Box::new(ast::Expression::Column("a".to_string())),
            Box::new(ast::Expression::Value(ast::Value::Integral(1))),
        ));
        let group_by_ref = ast::GroupByReference::new(vec!["b".to_string()], None);
        let group_by_expr = ast::GroupByExpression::new(vec![group_by_ref], None);
        let path_expr = PathExpr::new(vec![PathSegment::AttrName("it".to_string())]);
        let table_reference = ast::TableReference::new(path_expr, None, None);
        let before = ast::SelectStatement::new(
            select_exprs,
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
            Box::new(types::Expression::Variable("a".to_string())),
            Box::new(types::Expression::Constant(common::Value::Int(1))),
        ));

        let path_expr = PathExpr::new(vec![PathSegment::AttrName("it".to_string())]);
        let _binding = common::Binding {
            path_expr,
            name: "e".to_string(),
        };

        let filter = types::Node::Filter(
            filtered_formula,
            Box::new(types::Node::Map(
                vec![
                    types::Named::Expression(types::Expression::Variable("a".to_string()), Some("a".to_string())),
                    types::Named::Expression(types::Expression::Variable("b".to_string()), Some("b".to_string())),
                ],
                Box::new(types::Node::DataSource(
                    common::DataSource::Stdin("jsonl".to_string(), "it".to_string()),
                    vec![],
                )),
            )),
        );

        let named_aggregates = vec![types::NamedAggregate::new(
            types::Aggregate::Avg(types::Named::Expression(
                types::Expression::Variable("a".to_string()),
                Some("a".to_string()),
            )),
            None,
        )];

        let fields = vec!["b".to_string()];
        let expected = types::Node::GroupBy(fields, named_aggregates, Box::new(filter));

        let ans = parse_query(before, data_source).unwrap();
        assert_eq!(expected, ans);
    }

    #[test]
    fn test_parse_query_group_by_without_aggregate() {
        let select_exprs = vec![
            ast::SelectExpression::Expression(Box::new(ast::Expression::Column("a".to_string())), None),
            ast::SelectExpression::Expression(Box::new(ast::Expression::Column("b".to_string())), None),
        ];

        let where_expr = ast::WhereExpression::new(ast::Expression::BinaryOperator(
            ast::BinaryOperator::Equal,
            Box::new(ast::Expression::Column("a".to_string())),
            Box::new(ast::Expression::Value(ast::Value::Integral(1))),
        ));

        let group_by_ref = ast::GroupByReference::new(vec!["b".to_string()], None);
        let group_by_expr = ast::GroupByExpression::new(vec![group_by_ref], None);
        let path_expr = PathExpr::new(vec![PathSegment::AttrName("it".to_string())]);
        let table_reference = ast::TableReference::new(path_expr, None, None);
        let before = ast::SelectStatement::new(
            select_exprs,
            vec![table_reference],
            Some(where_expr),
            Some(group_by_expr),
            None,
            None,
            None,
        );
        let data_source = common::DataSource::Stdin("jsonl".to_string(), "it".to_string());
        let ans = parse_query(before, data_source);
        let expected = Err(ParseError::GroupByWithoutAggregateFunction);
        assert_eq!(expected, ans);
    }

    #[test]
    fn test_parse_query_group_by_mismatch_fields() {
        let select_exprs = vec![
            ast::SelectExpression::Expression(Box::new(ast::Expression::Column("a".to_string())), None),
            ast::SelectExpression::Expression(Box::new(ast::Expression::Column("b".to_string())), None),
            ast::SelectExpression::Expression(
                Box::new(ast::Expression::FuncCall(
                    "count".to_string(),
                    vec![ast::SelectExpression::Expression(
                        Box::new(ast::Expression::Column("c".to_string())),
                        None,
                    )],
                    None,
                )),
                None,
            ),
        ];

        let group_by_ref = ast::GroupByReference::new(vec!["b".to_string()], None);
        let group_by_expr = ast::GroupByExpression::new(vec![group_by_ref], None);
        let path_expr = PathExpr::new(vec![PathSegment::AttrName("it".to_string())]);
        let table_reference = ast::TableReference::new(path_expr, None, None);
        let before = ast::SelectStatement::new(
            select_exprs,
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

    #[test]
    fn test_parse_query_group_by_conflict_naming() {
        let select_exprs = vec![
            ast::SelectExpression::Expression(
                Box::new(ast::Expression::Column("a".to_string())),
                Some("t".to_string()),
            ),
            ast::SelectExpression::Expression(
                Box::new(ast::Expression::Column("b".to_string())),
                Some("t".to_string()),
            ),
        ];

        let path_expr = PathExpr::new(vec![PathSegment::AttrName("it".to_string())]);
        let table_reference = ast::TableReference::new(path_expr, None, None);
        let before = ast::SelectStatement::new(select_exprs, vec![table_reference], None, None, None, None, None);
        let data_source = common::DataSource::Stdin("jsonl".to_string(), "it".to_string());
        let ans = parse_query(before, data_source);
        let expected = Err(ParseError::ConflictVariableNaming);
        assert_eq!(expected, ans);
    }
}
