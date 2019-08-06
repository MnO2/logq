use super::types;
use crate::common::types as common;
use crate::syntax::ast;
use std::convert::TryFrom;

#[derive(Fail, Debug, Clone, PartialEq, Eq)]
pub enum ParseError {
    #[fail(display = "Type Mismatch")]
    TypeMismatch,
    #[fail(display = "Unsupported Logic Operator")]
    UnsupportedLogicOperator,
    #[fail(display = "Not Aggregate Function")]
    NotAggregateFunction,
    #[fail(display = "Select Expression Must Be Named")]
    SelectExprMustBeNamed,
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
        ast::Expression::Condition(c) => parse_condition(c),
        ast::Expression::And(l, r) => parse_infix_operator(types::LogicInfixOp::And, l, r),
        ast::Expression::Or(l, r) => parse_infix_operator(types::LogicInfixOp::Or, l, r),
        ast::Expression::Not(c) => parse_prefix_operator(types::LogicPrefixOp::Not, c),
        ast::Expression::Value(value_expr) => parse_boolean_value(value_expr),
    }
}

fn parse_logic_expression(expr: &ast::Expression) -> ParseResult<Box<types::Expression>> {
    let formula = parse_logic(expr)?;
    Ok(Box::new(types::Expression::LogicExpression(formula)))
}

fn parse_boolean_value(value_expr: &ast::ValueExpression) -> ParseResult<Box<types::Formula>> {
    match value_expr {
        ast::ValueExpression::Value(val) => match val {
            ast::Value::Boolean(b) => Ok(Box::new(types::Formula::Constant(*b))),
            _ => Err(ParseError::TypeMismatch),
        },
        _ => Err(ParseError::TypeMismatch),
    }
}

fn parse_value(value: &ast::Value) -> ParseResult<Box<types::Expression>> {
    match value {
        ast::Value::Boolean(b) => Ok(Box::new(types::Expression::Constant(common::Value::Boolean(*b)))),
        ast::Value::Float(f) => Ok(Box::new(types::Expression::Constant(common::Value::Float(*f)))),
        ast::Value::Integral(i) => Ok(Box::new(types::Expression::Constant(common::Value::Int(*i)))),
        ast::Value::StringLiteral(s) => Ok(Box::new(types::Expression::Constant(common::Value::String(s.clone())))),
    }
}

fn parse_arithemetic(value_expr: &ast::ValueExpression) -> ParseResult<Box<types::Expression>> {
    match value_expr {
        ast::ValueExpression::Operator(op, left_expr, right_expr) => {
            let func_name = (*op).to_string();
            let left = parse_value_expression(left_expr)?;
            let right = parse_value_expression(right_expr)?;
            let args = vec![left, right];
            Ok(Box::new(types::Expression::FunctionExpression(func_name, args)))
        }
        _ => {
            unimplemented!();
        }
    }
}

fn parse_value_expression(value_expr: &ast::ValueExpression) -> ParseResult<Box<types::Expression>> {
    match value_expr {
        ast::ValueExpression::Value(v) => {
            let expr = parse_value(v)?;
            Ok(expr)
        }
        ast::ValueExpression::Column(column_name) => Ok(Box::new(types::Expression::Variable(column_name.clone()))),
        ast::ValueExpression::Operator(_, _, _) => parse_arithemetic(value_expr),
        ast::ValueExpression::FuncCall(func_name, select_exprs) => {
            let mut args = Vec::new();
            for select_expr in select_exprs.iter() {
                let arg = parse_expression(select_expr)?;
                args.push(arg);
            }
            Ok(Box::new(types::Expression::FunctionExpression(func_name.clone(), args)))
        }
    }
}

fn parse_relation(op: &ast::RelationOperator) -> ParseResult<types::Relation> {
    match op {
        ast::RelationOperator::Equal => Ok(types::Relation::Equal),
        ast::RelationOperator::NotEqual => Ok(types::Relation::NotEqual),
        ast::RelationOperator::GreaterEqual => Ok(types::Relation::GreaterEqual),
        ast::RelationOperator::LessEqual => Ok(types::Relation::LessEqual),
        ast::RelationOperator::LessThan => Ok(types::Relation::LessThan),
        ast::RelationOperator::MoreThan => Ok(types::Relation::MoreThan),
    }
}

fn parse_condition(condition: &ast::Condition) -> ParseResult<Box<types::Formula>> {
    match condition {
        ast::Condition::ComparisonExpression(op, left_expr, right_expr) => {
            let left = parse_value_expression(left_expr)?;
            let right = parse_value_expression(right_expr)?;
            let rel_op = parse_relation(op)?;
            Ok(Box::new(types::Formula::Predicate(rel_op, left, right)))
        }
    }
}

fn parse_expression(select_expr: &ast::SelectExpression) -> ParseResult<Box<types::Expression>> {
    match select_expr {
        ast::SelectExpression::Star => unimplemented!(),
        ast::SelectExpression::Expression(expr) => match &**expr {
            ast::Expression::Condition(c) => {
                let formula = parse_condition(c)?;
                let logic_expression = types::Expression::LogicExpression(formula);
                Ok(Box::new(logic_expression))
            }
            ast::Expression::And(_, _) => parse_logic_expression(expr),
            ast::Expression::Or(_, _) => parse_logic_expression(expr),
            ast::Expression::Not(_) => parse_logic_expression(expr),
            ast::Expression::Value(value_expr) => parse_value_expression(value_expr),
        },
    }
}

impl TryFrom<&str> for types::AggregateFunction {
    type Error = ParseError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        match value {
            "avg" => Ok(types::AggregateFunction::Avg),
            "count" => Ok(types::AggregateFunction::Count),
            "first" => Ok(types::AggregateFunction::First),
            "last" => Ok(types::AggregateFunction::Last),
            "max" => Ok(types::AggregateFunction::Max),
            "min" => Ok(types::AggregateFunction::Min),
            "sum" => Ok(types::AggregateFunction::Sum),
            _ => Err(ParseError::NotAggregateFunction),
        }
    }
}

fn parse_aggregate(select_expr: &ast::SelectExpression) -> ParseResult<types::Aggregate> {
    match select_expr {
        ast::SelectExpression::Expression(expr) => match &**expr {
            ast::Expression::Value(value_expr) => match &**value_expr {
                ast::ValueExpression::FuncCall(func_name, args) => {
                    let argument = *parse_expression(&args[0])?;
                    let aggregate_func = types::AggregateFunction::try_from(&**func_name)?;

                    match &argument {
                        types::Expression::Variable(name) => {
                            let named = types::Named::Expression(argument.clone(), name.clone());
                            Ok(types::Aggregate::new(aggregate_func, named))
                        }
                        _ => Err(ParseError::SelectExprMustBeNamed),
                    }
                }
                _ => Err(ParseError::TypeMismatch),
            },
            _ => Err(ParseError::TypeMismatch),
        },
        _ => Err(ParseError::TypeMismatch),
    }
}

pub(crate) fn parse_query(query: ast::SelectStatement, data_source: types::DataSource) -> ParseResult<types::Node> {
    let mut root = types::Node::DataSource(data_source);

    let mut aggregates = Vec::new();
    if !query.select_exprs.is_empty() {
        let mut named_list: Vec<types::Named> = Vec::new();
        for select_expr in query.select_exprs.iter() {
            let parse_aggregate_result = parse_aggregate(select_expr);
            if parse_aggregate_result.is_ok() {
                let aggregate = parse_aggregate_result.unwrap();
                aggregates.push(aggregate.clone());

                let types::Aggregate { argument, .. } = aggregate;

                named_list.push(argument);
            } else {
                let expression = *parse_expression(select_expr)?;
                match &expression {
                    types::Expression::Variable(name) => {
                        let named = types::Named::Expression(expression.clone(), name.clone());
                        named_list.push(named);
                    }
                    _ => return Err(ParseError::SelectExprMustBeNamed),
                }
            }
        }

        root = types::Node::Map(named_list, Box::new(root));
    }

    if let Some(where_expr) = query.where_expr_opt {
        let filter_formula = parse_logic(&where_expr.expr)?;
        root = types::Node::Filter(filter_formula, Box::new(root));
    }

    if let Some(group_by) = query.group_by_exprs_opt {
        let fields = group_by.exprs.clone();
        root = types::Node::GroupBy(fields, aggregates, Box::new(root));
    }

    Ok(root)
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_parse_logic_expression() {
        let before = ast::Expression::And(
            Box::new(ast::Expression::Value(Box::new(ast::ValueExpression::Value(
                ast::Value::Boolean(true),
            )))),
            Box::new(ast::Expression::Value(Box::new(ast::ValueExpression::Value(
                ast::Value::Boolean(false),
            )))),
        );

        let expected = Box::new(types::Expression::LogicExpression(Box::new(
            types::Formula::InfixOperator(
                types::LogicInfixOp::And,
                Box::new(types::Formula::Constant(true)),
                Box::new(types::Formula::Constant(false)),
            ),
        )));

        let ans = parse_logic_expression(&before).unwrap();
        assert_eq!(expected, ans);

        let before = ast::Expression::Not(Box::new(ast::Expression::Value(Box::new(ast::ValueExpression::Value(
            ast::Value::Boolean(false),
        )))));

        let expected = Box::new(types::Expression::LogicExpression(Box::new(
            types::Formula::PrefixOperator(types::LogicPrefixOp::Not, Box::new(types::Formula::Constant(false))),
        )));

        let ans = parse_logic_expression(&before).unwrap();
        assert_eq!(expected, ans);
    }

    #[test]
    fn test_parse_value_expression() {
        let before = ast::ValueExpression::Operator(
            ast::ValueOperator::Plus,
            Box::new(ast::ValueExpression::Operator(
                ast::ValueOperator::Plus,
                Box::new(ast::ValueExpression::Value(ast::Value::Integral(1))),
                Box::new(ast::ValueExpression::Value(ast::Value::Integral(2))),
            )),
            Box::new(ast::ValueExpression::Value(ast::Value::Integral(3))),
        );

        let expected = Box::new(types::Expression::FunctionExpression(
            "Plus".to_string(),
            vec![
                Box::new(types::Expression::FunctionExpression(
                    "Plus".to_string(),
                    vec![
                        Box::new(types::Expression::Constant(common::Value::Int(1))),
                        Box::new(types::Expression::Constant(common::Value::Int(2))),
                    ],
                )),
                Box::new(types::Expression::Constant(common::Value::Int(3))),
            ],
        ));

        let ans = parse_value_expression(&before).unwrap();
        assert_eq!(expected, ans);
    }

    #[test]
    fn test_parse_aggregate() {
        let before = ast::SelectExpression::Expression(Box::new(ast::Expression::Value(Box::new(
            ast::ValueExpression::FuncCall(
                "avg".to_string(),
                vec![ast::SelectExpression::Expression(Box::new(ast::Expression::Value(
                    Box::new(ast::ValueExpression::Column("a".to_string())),
                )))],
            ),
        ))));

        let argument = types::Named::Expression(types::Expression::Variable("a".to_string()), "a".to_string());
        let expected = types::Aggregate::new(types::AggregateFunction::Avg, argument);

        let ans = parse_aggregate(&before).unwrap();
        assert_eq!(expected, ans);
    }

    #[test]
    fn test_parse_condition() {
        let before = ast::Condition::ComparisonExpression(
            ast::RelationOperator::Equal,
            Box::new(ast::ValueExpression::Column("a".to_string())),
            Box::new(ast::ValueExpression::Value(ast::Value::Integral(1))),
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
            ast::SelectExpression::Expression(Box::new(ast::Expression::Value(Box::new(
                ast::ValueExpression::Column("a".to_string()),
            )))),
            ast::SelectExpression::Expression(Box::new(ast::Expression::Value(Box::new(
                ast::ValueExpression::Column("b".to_string()),
            )))),
        ];

        let where_expr = ast::WhereExpression::new(ast::Expression::Condition(ast::Condition::ComparisonExpression(
            ast::RelationOperator::Equal,
            Box::new(ast::ValueExpression::Column("a".to_string())),
            Box::new(ast::ValueExpression::Value(ast::Value::Integral(1))),
        )));
        //let group_by_expr = ast::GroupByExpression::new(vec!["b".to_string()]);

        let before = ast::SelectStatement::new(select_exprs, Some(where_expr), None);
        let data_source = types::DataSource::Stdin;

        let filtered_formula = Box::new(types::Formula::Predicate(
            types::Relation::Equal,
            Box::new(types::Expression::Variable("a".to_string())),
            Box::new(types::Expression::Constant(common::Value::Int(1))),
        ));

        let expected = types::Node::Filter(
            filtered_formula,
            Box::new(types::Node::Map(
                vec![
                    types::Named::Expression(types::Expression::Variable("a".to_string()), "a".to_string()),
                    types::Named::Expression(types::Expression::Variable("b".to_string()), "b".to_string()),
                ],
                Box::new(types::Node::DataSource(types::DataSource::Stdin)),
            )),
        );

        let ans = parse_query(before, data_source).unwrap();
        assert_eq!(expected, ans);
    }

    #[test]
    fn test_parse_query_with_group_by() {
        let select_exprs = vec![
            ast::SelectExpression::Expression(Box::new(ast::Expression::Value(Box::new(
                ast::ValueExpression::FuncCall(
                    "avg".to_string(),
                    vec![ast::SelectExpression::Expression(Box::new(ast::Expression::Value(
                        Box::new(ast::ValueExpression::Column("a".to_string())),
                    )))],
                ),
            )))),
            ast::SelectExpression::Expression(Box::new(ast::Expression::Value(Box::new(
                ast::ValueExpression::FuncCall(
                    "count".to_string(),
                    vec![ast::SelectExpression::Expression(Box::new(ast::Expression::Value(
                        Box::new(ast::ValueExpression::Column("b".to_string())),
                    )))],
                ),
            )))),
        ];

        let where_expr = ast::WhereExpression::new(ast::Expression::Condition(ast::Condition::ComparisonExpression(
            ast::RelationOperator::Equal,
            Box::new(ast::ValueExpression::Column("a".to_string())),
            Box::new(ast::ValueExpression::Value(ast::Value::Integral(1))),
        )));
        let group_by_expr = ast::GroupByExpression::new(vec!["b".to_string()]);

        let before = ast::SelectStatement::new(select_exprs, Some(where_expr), Some(group_by_expr));
        let data_source = types::DataSource::Stdin;

        let filtered_formula = Box::new(types::Formula::Predicate(
            types::Relation::Equal,
            Box::new(types::Expression::Variable("a".to_string())),
            Box::new(types::Expression::Constant(common::Value::Int(1))),
        ));

        let filter = types::Node::Filter(
            filtered_formula,
            Box::new(types::Node::Map(
                vec![
                    types::Named::Expression(types::Expression::Variable("a".to_string()), "a".to_string()),
                    types::Named::Expression(types::Expression::Variable("b".to_string()), "b".to_string()),
                ],
                Box::new(types::Node::DataSource(types::DataSource::Stdin)),
            )),
        );

        let aggregates = vec![
            types::Aggregate::new(
                types::AggregateFunction::Avg,
                types::Named::Expression(types::Expression::Variable("a".to_string()), "a".to_string()),
            ),
            types::Aggregate::new(
                types::AggregateFunction::Count,
                types::Named::Expression(types::Expression::Variable("b".to_string()), "b".to_string()),
            ),
        ];

        let fields = vec!["b".to_string()];
        let expected = types::Node::GroupBy(fields, aggregates, Box::new(filter));

        let ans = parse_query(before, data_source).unwrap();
        assert_eq!(expected, ans);
    }
}
