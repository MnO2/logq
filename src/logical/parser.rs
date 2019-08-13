use super::types;
use crate::common::types as common;
use crate::execution;
use crate::syntax::ast;
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
                                    types::Aggregate::PercentileDisc(
                                        f,
                                        within_group_clause.ordering_term.column_name.clone(),
                                        o,
                                    )
                                }
                                _ => {
                                    unimplemented!();
                                }
                            },
                            _ => {
                                unimplemented!();
                            }
                        },
                        _ => {
                            unimplemented!();
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

pub(crate) fn parse_query(query: ast::SelectStatement, data_source: common::DataSource) -> ParseResult<types::Node> {
    let mut root = types::Node::DataSource(data_source);
    let mut named_aggregates = Vec::new();
    let mut named_list: Vec<types::Named> = Vec::new();
    let mut non_aggregates: Vec<types::Named> = Vec::new();

    if !query.select_exprs.is_empty() {
        for select_expr in query.select_exprs.iter() {
            let parse_aggregate_result = parse_aggregate(select_expr);
            if parse_aggregate_result.is_ok() {
                let named_aggregate = parse_aggregate_result.unwrap();
                named_aggregates.push(named_aggregate.clone());

                match named_aggregate.aggregate {
                    types::Aggregate::Avg(named) => {
                        named_list.push(named);
                    }
                    types::Aggregate::Count(named) => {
                        named_list.push(named);
                    }
                    types::Aggregate::First(named) => {
                        named_list.push(named);
                    }
                    types::Aggregate::Last(named) => {
                        named_list.push(named);
                    }
                    types::Aggregate::Sum(named) => {
                        named_list.push(named);
                    }
                    types::Aggregate::Max(named) => {
                        named_list.push(named);
                    }
                    types::Aggregate::Min(named) => {
                        named_list.push(named);
                    }
                    types::Aggregate::PercentileDisc(_, column_name, _) => named_list.push(types::Named::Expression(
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
            let fields = group_by.exprs.clone();

            if !is_match_group_by_fields(&fields, &non_aggregates) {
                return Err(ParseError::GroupByFieldsMismatch);
            }

            root = types::Node::GroupBy(fields, named_aggregates, Box::new(root));
        } else {
            let fields = Vec::new();
            root = types::Node::GroupBy(fields, named_aggregates, Box::new(root));
        }
    } else {
        //sanity check if there is a group by statement
        if query.group_by_exprs_opt.is_some() {
            return Err(ParseError::GroupByWithoutAggregateFunction);
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

fn is_match_group_by_fields(variables: &[common::VariableName], named_list: &[types::Named]) -> bool {
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
                for field_name in execution::datasource::ClassicLoadBalancerLogField::field_names().into_iter() {
                    b.push(field_name);
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
    fn test_parse_query_with_simple_select_where() {
        let select_exprs = vec![
            ast::SelectExpression::Expression(Box::new(ast::Expression::Column("a".to_string())), None),
            ast::SelectExpression::Expression(Box::new(ast::Expression::Column("b".to_string())), None),
        ];

        let where_expr = ast::WhereExpression::new(ast::Expression::BinaryOperator(
            ast::BinaryOperator::Equal,
            Box::new(ast::Expression::Column("a".to_string())),
            Box::new(ast::Expression::Value(ast::Value::Integral(1))),
        ));

        let before = ast::SelectStatement::new(select_exprs, "elb", Some(where_expr), None, None, None);
        let data_source = common::DataSource::Stdin;

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
                Box::new(types::Node::DataSource(common::DataSource::Stdin)),
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
        let group_by_expr = ast::GroupByExpression::new(vec!["b".to_string()]);

        let before = ast::SelectStatement::new(select_exprs, "elb", Some(where_expr), Some(group_by_expr), None, None);
        let data_source = common::DataSource::Stdin;

        let filtered_formula = Box::new(types::Formula::Predicate(
            types::Relation::Equal,
            Box::new(types::Expression::Variable("a".to_string())),
            Box::new(types::Expression::Constant(common::Value::Int(1))),
        ));

        let filter = types::Node::Filter(
            filtered_formula,
            Box::new(types::Node::Map(
                vec![
                    types::Named::Expression(types::Expression::Variable("a".to_string()), Some("a".to_string())),
                    types::Named::Expression(types::Expression::Variable("b".to_string()), Some("b".to_string())),
                ],
                Box::new(types::Node::DataSource(common::DataSource::Stdin)),
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
        let group_by_expr = ast::GroupByExpression::new(vec!["b".to_string()]);

        let before = ast::SelectStatement::new(select_exprs, "elb", Some(where_expr), Some(group_by_expr), None, None);
        let data_source = common::DataSource::Stdin;
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

        let group_by_expr = ast::GroupByExpression::new(vec!["b".to_string()]);

        let before = ast::SelectStatement::new(select_exprs, "elb", None, Some(group_by_expr), None, None);
        let data_source = common::DataSource::Stdin;
        let ans = parse_query(before, data_source);
        let expected = Err(ParseError::GroupByFieldsMismatch);
        assert_eq!(expected, ans);
    }
}
