use super::ast;

use nom::{
    branch::alt,
    bytes::complete::{escaped, tag, take_while},
    character::complete::{alphanumeric1 as alphanumeric, char, digit1, multispace0, multispace1, one_of, space0},
    combinator::{cut, map, map_res, opt},
    error::{context, VerboseError},
    multi::{fold_many0, separated_list},
    number::complete::double,
    sequence::{delimited, pair, preceded, separated_pair, terminated, tuple},
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
        |(l, r)| ast::Condition::ComparisonExpression(ast::RelationOperator::Equal, Box::new(l), Box::new(r)),
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
    alt((map(condition, |c| ast::Expression::Condition(c)), expression_term))(i)
}

fn select_expression<'a>(i: &'a str) -> IResult<&'a str, ast::SelectExpression, VerboseError<&'a str>> {
    preceded(
        space0,
        alt((
            map(char('*'), |_| ast::SelectExpression::Star),
            map(expression, |e| ast::SelectExpression::Expression(Box::new(e))),
        )),
    )(i)
}

fn select_expression_list<'a>(i: &'a str) -> IResult<&'a str, Vec<ast::SelectExpression>, VerboseError<&'a str>> {
    context(
        "select_expression_list",
        terminated(separated_list(preceded(space0, char(',')), select_expression), space0),
    )(i)
}

fn where_expression<'a>(i: &'a str) -> IResult<&'a str, ast::WhereExpression, VerboseError<&'a str>> {
    map(preceded(tag("where"), expression), |e| ast::WhereExpression::new(e))(i)
}

pub(crate) fn select_query<'a>(i: &'a str) -> IResult<&'a str, ast::SelectStatement, VerboseError<&'a str>> {
    map(
        pair(select_expression_list, opt(where_expression)),
        |(select_exprs, where_expr)| ast::SelectStatement::new(select_exprs, where_expr),
    )(i)
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_boolean() {
        assert_eq!(boolean("true"), Ok(("", ast::Value::Boolean(true))));
        assert_eq!(boolean("false"), Ok(("", ast::Value::Boolean(false))));
    }

    #[test]
    fn test_string_literal() {
        assert_eq!(string_literal("abc"), Ok(("", "abc")));
        assert_eq!(string_literal("def"), Ok(("", "def")));
    }

    #[test]
    fn test_number() {
        assert_eq!(number("123"), Ok(("", ast::Value::Number(123))));
        assert_eq!(number("-123"), Ok(("", ast::Value::Number(-123))));
    }

    #[test]
    fn test_where_value_expression() {
        let ans = ast::ValueExpression::Operator(
            ast::ValueOperator::Plus,
            Box::new(ast::ValueExpression::Operator(
                ast::ValueOperator::Plus,
                Box::new(ast::ValueExpression::Value(ast::Value::Number(1))),
                Box::new(ast::ValueExpression::Value(ast::Value::Number(2))),
            )),
            Box::new(ast::ValueExpression::Value(ast::Value::Number(3))),
        );
        assert_eq!(value_expression("1 + 2 + 3"), Ok(("", ans)));
    }

    #[test]
    fn test_condition_expression() {
        let ans = ast::Condition::ComparisonExpression(
            ast::RelationOperator::Equal,
            Box::new(ast::ValueExpression::Operator(
                ast::ValueOperator::Plus,
                Box::new(ast::ValueExpression::Value(ast::Value::Number(1))),
                Box::new(ast::ValueExpression::Value(ast::Value::Number(2))),
            )),
            Box::new(ast::ValueExpression::Value(ast::Value::Number(3))),
        );

        assert_eq!(condition("1 + 2 = 3"), Ok(("", ans)));
    }

    #[test]
    fn test_expression_term() {
        let ans = ast::Expression::And(
            Box::new(ast::Expression::Value(Box::new(ast::ValueExpression::Value(
                ast::Value::Boolean(true),
            )))),
            Box::new(ast::Expression::Value(Box::new(ast::ValueExpression::Value(
                ast::Value::Boolean(true),
            )))),
        );
        assert_eq!(expression_term("true AND true"), Ok(("", ans)));
    }

    #[test]
    fn test_select_expression_list() {
        let ans = vec![
            ast::SelectExpression::Star,
            ast::SelectExpression::Star,
            ast::SelectExpression::Star,
        ];
        assert_eq!(select_expression_list("*, *, *"), Ok(("", ans)));

        let ans = vec![
            ast::SelectExpression::Expression(Box::new(ast::Expression::Value(Box::new(ast::ValueExpression::Value(
                ast::Value::Number(1),
            ))))),
            ast::SelectExpression::Expression(Box::new(ast::Expression::Value(Box::new(ast::ValueExpression::Value(
                ast::Value::Number(2),
            ))))),
            ast::SelectExpression::Expression(Box::new(ast::Expression::Value(Box::new(ast::ValueExpression::Value(
                ast::Value::Number(3),
            ))))),
        ];
        assert_eq!(select_expression_list("1, 2, 3"), Ok(("", ans)));
    }
}
