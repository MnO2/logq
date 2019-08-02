use super::ast;
use super::lexer::Lexer;
use super::token::Token;

use nom::{
    branch::alt,
    bytes::complete::{escaped, tag, take_while},
    character::complete::{alphanumeric1 as alphanumeric, char, digit1, multispace0, multispace1, one_of, space0},
    combinator::{cut, map, map_res, opt},
    error::{context, VerboseError},
    multi::{fold_many0, separated_list},
    number::complete::double,
    sequence::{delimited, pair, separated_pair, preceded, terminated, tuple},
    IResult,
};

fn string_literal<'a>(i: &'a str) -> IResult<&'a str, &'a str, VerboseError<&'a str>> {
    escaped(alphanumeric, '\\', one_of("\"n\\"))(i)
}

fn boolean<'a>(i: &'a str) -> IResult<&'a str, ast::Value, VerboseError<&'a str>> {
    alt((
        map(tag("true"), |_| ast::Value::Boolean(true)),
        map(tag("false"), |_| ast::Value::Boolean(false)),
    ))(i)
}

fn number<'a>(i: &'a str) -> IResult<&'a str, ast::Value, VerboseError<&'a str>> {
    alt((
        map_res(digit1, |digit_str: &str| {
            digit_str.parse::<i32>().map(ast::Value::Number)
        }),
        map(preceded(tag("-"), digit1), |digit_str: &str| {
            ast::Value::Number(-1 * digit_str.parse::<i32>().unwrap())
        }),
    ))(i)
}

fn value<'a>(i: &'a str) -> IResult<&'a str, ast::Value, VerboseError<&'a str>> {
    alt((number, boolean))(i)
}

fn parens<'a>(i: &'a str) -> IResult<&'a str, ast::ValueExpression, VerboseError<&'a str>> {
    delimited(space0, delimited(tag("("), value_expression, tag(")")), space0)(i)
}

fn factor<'a>(i: &'a str) -> IResult<&'a str, ast::ValueExpression, VerboseError<&'a str>> {
    alt((
        delimited(space0, map(value, |v| ast::ValueExpression::Value(v)), space0),
        parens,
    ))(i)
}

fn term<'a>(i: &'a str) -> IResult<&'a str, ast::ValueExpression, VerboseError<&'a str>> {
    let (i, init) = factor(i)?;

    fold_many0(
        pair(alt((char('*'), char('/'))), factor),
        init,
        |acc, (op, val): (char, ast::ValueExpression)| {
            if op == '*' {
                ast::ValueExpression::Operator(ast::ValueOperator::Times, Box::new(acc), Box::new(val))
            } else {
                ast::ValueExpression::Operator(ast::ValueOperator::Divide, Box::new(acc), Box::new(val))
            }
        },
    )(i)
}

fn value_expression<'a>(i: &'a str) -> IResult<&'a str, ast::ValueExpression, VerboseError<&'a str>> {
    let (i, init) = term(i)?;

    fold_many0(
        pair(alt((char('+'), char('-'))), term),
        init,
        |acc, (op, val): (char, ast::ValueExpression)| {
            if op == '+' {
                ast::ValueExpression::Operator(ast::ValueOperator::Plus, Box::new(acc), Box::new(val))
            } else {
                ast::ValueExpression::Operator(ast::ValueOperator::Minus, Box::new(acc), Box::new(val))
            }
        },
    )(i)
}

fn condition<'a>(i: &'a str) -> IResult<&'a str, ast::Condition, VerboseError<&'a str>> {
    map(
        separated_pair(value_expression, alt((tag("="), tag("/="))), value_expression), 
        |(l, r)| ast::Condition::ComparisonExpression(ast::RelationOperator::Equal, Box::new(l), Box::new(r))
    )(i)
}

fn expression_term<'a>(i: &'a str) -> IResult<&'a str, ast::Expression, VerboseError<&'a str>> {
    let (i, init) = value_expression(i)?;

    fold_many0(
        pair(alt((tag("OR"), tag("AND"))), value_expression),
        ast::Expression::Value(Box::new(init)),
        |acc, (op, val): (&str, ast::ValueExpression)| {
            if op == "AND" {
                ast::Expression::And(Box::new(acc), Box::new(ast::Expression::Value(Box::new(val))))
            } else {
                ast::Expression::Or(Box::new(acc), Box::new(ast::Expression::Value(Box::new(val))))
            }
        },
    )(i)
}

fn expression<'a>(i: &'a str) -> IResult<&'a str, ast::Expression, VerboseError<&'a str>> {
    alt((
        map(condition, |c| ast::Expression::Condition(c)),
        expression_term,
    ))(i)
}

fn select_expression<'a>(i: &'a str) -> IResult<&'a str, ast::SelectExpression, VerboseError<&'a str>> {
    alt((
        map(char('*'), |_| ast::SelectExpression::Star),
        map(expression, |e| ast::SelectExpression::Expression(Box::new(e))),
    ))(i)
}

fn select_expression_list<'a>(i: &'a str) -> IResult<&'a str, Vec<ast::SelectExpression>, VerboseError<&'a str>> {
    context(
        "select_expression_list",
        separated_list(preceded(multispace1, char(',')), select_expression)
    )(i)
}

fn where_expression<'a>(i: &'a str) -> IResult<&'a str, ast::WhereExpression, VerboseError<&'a str>> {
    map(preceded(tag("where"), expression), |e| ast::WhereExpression::new(e))(i)
}

pub(crate) fn select_query<'a>(i: &'a str) -> IResult<&'a str, ast::SelectStatement, VerboseError<&'a str>> {
    map(
        pair(select_expression_list, opt(where_expression)),
        |(select_exprs, where_expr)| ast::SelectStatement::new(select_exprs, where_expr)
    )(i)
}

#[cfg(test)]
mod test {
    use super::super::lexer::Lexer;
    use super::*;
}
