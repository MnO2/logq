#![feature(box_patterns)]

use super::types;
use crate::common::types as common;
use crate::syntax::ast;

#[derive(Fail, Debug, Clone, PartialEq, Eq)]
pub enum ParseError {
    #[fail(display = "Type Mismatch")]
    TypeMismatch,
    #[fail(display = "Unsupported Logic Operator")]
    UnsupportedLogicOperator,
}

pub type ParseResult<T> = Result<T, ParseError>;

fn parse_prefix_operator(operator: &str, child: &ast::Expression) -> ParseResult<Box<types::Formula>> {
    let child_parsed = parse_logic(child)?;

    let prefix_op = types::Formula::PrefixOperator(operator.to_string(), child_parsed);
    Ok(Box::new(prefix_op))
}

fn parse_infix_operator(
    operator: &str,
    left: &ast::Expression,
    right: &ast::Expression,
) -> ParseResult<Box<types::Formula>> {
    let left_parsed = parse_logic(left)?;
    let right_parsed = parse_logic(right)?;

    let infix_op = types::Formula::InfixOperator(operator.to_string(), left_parsed, right_parsed);
    Ok(Box::new(infix_op))
}

fn parse_logic(expr: &ast::Expression) -> ParseResult<Box<types::Formula>> {
    match expr {
        ast::Expression::And(l, r) => parse_infix_operator("AND", l, r),
        ast::Expression::Or(l, r) => parse_infix_operator("OR", l, r),
        ast::Expression::Not(c) => parse_prefix_operator("NOT", c),
        ast::Expression::Value(value_expr) => match &**value_expr {
            ast::ValueExpression::Value(v) => match v {
                ast::Value::Boolean(b) => parse_boolean(*b),
                _ => Err(ParseError::TypeMismatch),
            },
            _ => Err(ParseError::TypeMismatch),
        },
        _ => Err(ParseError::UnsupportedLogicOperator),
    }
}

fn parse_logic_expression(expr: &ast::Expression) -> ParseResult<Box<types::Expression>> {
    let formula = parse_logic(expr)?;
    Ok(Box::new(types::Expression::LogicExpression(formula)))
}

fn parse_boolean(b: bool) -> ParseResult<Box<types::Formula>> {
    Ok(Box::new(types::Formula::Constant(common::Value::Boolean(b))))
}

fn parse_expression(expr: &ast::Expression) -> ParseResult<Box<types::Expression>> {
    match expr {
        ast::Expression::Condition(c) => unimplemented!(),
        ast::Expression::And(_, _) => parse_logic_expression(expr),
        ast::Expression::Or(_, _) => parse_logic_expression(expr),
        ast::Expression::Not(_) => parse_logic_expression(expr),
        _ => Err(ParseError::TypeMismatch),
    }
}

fn parse_query(query: ast::SelectStatement, data_source: types::DataSource) -> ParseResult<types::Node> {
    let root = types::Node::DataSource(data_source);

    if !query.select_exprs.is_empty() {
        for select_exprs in query.select_exprs.iter() {
            match select_exprs {
                ast::SelectExpression::Star => {}
                ast::SelectExpression::Expression(expr) => {
                    parse_expression(expr);
                }
            }
        }
    }

    if query.where_expr_opt.is_some() {}

    if query.group_by_expr_opt.is_some() {}

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
                "AND".to_string(),
                Box::new(types::Formula::Constant(common::Value::Boolean(true))),
                Box::new(types::Formula::Constant(common::Value::Boolean(false))),
            ),
        )));

        let ans = parse_logic_expression(&before).unwrap();
        assert_eq!(expected, ans);
    }
}
