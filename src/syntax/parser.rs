use super::ast;
use ordered_float::OrderedFloat;

use nom::{
    branch::alt,
    bytes::complete::{escaped, tag},
    character::complete::{alpha1, char, digit1, none_of, one_of, space0, space1},
    combinator::{cut, map, map_res, opt},
    error::{context, VerboseError},
    multi::{fold_many0, separated_list},
    number::complete,
    sequence::{delimited, pair, preceded, separated_pair, terminated, tuple},
    AsChar, IResult, InputTakeAtPosition,
};

fn string_literal_interior<'a>(i: &'a str) -> IResult<&'a str, &'a str, VerboseError<&'a str>> {
    escaped(none_of("\""), '\\', one_of("\"n\\"))(i)
}

fn string_literal<'a>(i: &'a str) -> IResult<&'a str, ast::Value, VerboseError<&'a str>> {
    context(
        "string",
        map(
            preceded(char('\"'), cut(terminated(string_literal_interior, char('\"')))),
            |s| ast::Value::StringLiteral(s.to_string()),
        ),
    )(i)
}

fn identifier<T, E: nom::error::ParseError<T>>(input: T) -> IResult<T, T, E>
where
    T: InputTakeAtPosition,
    <T as InputTakeAtPosition>::Item: AsChar,
{
    fn is_alphanumeric_or_underscore(chr: char) -> bool {
        chr.is_alphabetic() || chr == '_'
    }

    input.split_at_position1_complete(
        |item| !is_alphanumeric_or_underscore(item.as_char()),
        nom::error::ErrorKind::Alpha,
    )
}

fn column_name<'a>(i: &'a str) -> IResult<&'a str, &'a str, VerboseError<&'a str>> {
    identifier(i)
}

fn boolean<'a>(i: &'a str) -> IResult<&'a str, ast::Value, VerboseError<&'a str>> {
    alt((
        map(tag("true"), |_| ast::Value::Boolean(true)),
        map(tag("false"), |_| ast::Value::Boolean(false)),
    ))(i)
}

fn float<'a>(i: &'a str) -> IResult<&'a str, ast::Value, VerboseError<&'a str>> {
    map(complete::float, |f| ast::Value::Float(OrderedFloat::from(f)))(i)
}

fn integral<'a>(i: &'a str) -> IResult<&'a str, ast::Value, VerboseError<&'a str>> {
    alt((
        map_res(digit1, |digit_str: &str| {
            digit_str.parse::<i32>().map(ast::Value::Integral)
        }),
        map(preceded(tag("-"), digit1), |digit_str: &str| {
            ast::Value::Integral(-digit_str.parse::<i32>().unwrap())
        }),
    ))(i)
}

fn value<'a>(i: &'a str) -> IResult<&'a str, ast::Value, VerboseError<&'a str>> {
    alt((integral, boolean, float, string_literal))(i)
}

fn parens<'a>(i: &'a str) -> IResult<&'a str, ast::ValueExpression, VerboseError<&'a str>> {
    delimited(space0, delimited(tag("("), value_expression, tag(")")), space0)(i)
}

fn order_by_clause_for_within_group<'a>(i: &'a str) -> IResult<&'a str, ast::OrderByExpression, VerboseError<&'a str>> {
    map(
        preceded(tuple((tag("order"), space1, tag("by"), space1)), ordering_term),
        |item| ast::OrderByExpression::new(vec![item]),
    )(i)
}

fn within_group_clause<'a>(i: &'a str) -> IResult<&'a str, ast::WithinGroupClause, VerboseError<&'a str>> {
    map(
        preceded(
            tuple((space1, tag("within"), space1, tag("group"), space1)),
            delimited(tag("("), order_by_clause_for_within_group, tag(")")),
        ),
        ast::WithinGroupClause::new,
    )(i)
}

fn func_call<'a>(i: &'a str) -> IResult<&'a str, ast::ValueExpression, VerboseError<&'a str>> {
    map(
        delimited(
            space0,
            tuple((
                identifier,
                delimited(tag("("), opt(select_expression_list), tag(")")),
                opt(within_group_clause),
            )),
            space0,
        ),
        |(func_name, select_expr_list_opt, within_group_opt)| {
            if let Some(select_expr_list) = select_expr_list_opt {
                ast::ValueExpression::FuncCall(func_name.to_string(), select_expr_list, within_group_opt)
            } else {
                ast::ValueExpression::FuncCall(func_name.to_string(), vec![], within_group_opt)
            }
        },
    )(i)
}

fn factor<'a>(i: &'a str) -> IResult<&'a str, ast::ValueExpression, VerboseError<&'a str>> {
    alt((
        func_call,
        delimited(space0, map(value, ast::ValueExpression::Value), space0),
        delimited(
            space0,
            map(column_name, |n| ast::ValueExpression::Column(n.to_string())),
            space0,
        ),
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
    alt((
        map(
            separated_pair(value_expression, tag("="), value_expression),
            |(l, r)| ast::Condition::ComparisonExpression(ast::RelationOperator::Equal, Box::new(l), Box::new(r)),
        ),
        map(
            separated_pair(value_expression, tag("/="), value_expression),
            |(l, r)| ast::Condition::ComparisonExpression(ast::RelationOperator::NotEqual, Box::new(l), Box::new(r)),
        ),
        map(
            separated_pair(value_expression, tag(">="), value_expression),
            |(l, r)| {
                ast::Condition::ComparisonExpression(ast::RelationOperator::GreaterEqual, Box::new(l), Box::new(r))
            },
        ),
        map(
            separated_pair(value_expression, tag("<="), value_expression),
            |(l, r)| ast::Condition::ComparisonExpression(ast::RelationOperator::LessEqual, Box::new(l), Box::new(r)),
        ),
        map(
            separated_pair(value_expression, tag(">"), value_expression),
            |(l, r)| ast::Condition::ComparisonExpression(ast::RelationOperator::MoreThan, Box::new(l), Box::new(r)),
        ),
        map(
            separated_pair(value_expression, tag("<"), value_expression),
            |(l, r)| ast::Condition::ComparisonExpression(ast::RelationOperator::LessThan, Box::new(l), Box::new(r)),
        ),
    ))(i)
}

fn expression_term_opt_not<'a>(i: &'a str) -> IResult<&'a str, ast::Expression, VerboseError<&'a str>> {
    map(
        pair(opt(preceded(space1, tag("NOT"))), value_expression),
        |(opt_not, val)| {
            if opt_not.is_some() {
                ast::Expression::Not(Box::new(ast::Expression::Value(Box::new(val))))
            } else {
                ast::Expression::Value(Box::new(val))
            }
        },
    )(i)
}

fn expression_term<'a>(i: &'a str) -> IResult<&'a str, ast::Expression, VerboseError<&'a str>> {
    let (i, init) = expression_term_opt_not(i)?;

    fold_many0(
        pair(alt((tag("OR"), tag("AND"))), expression_term_opt_not),
        init,
        |acc, (op, val): (&str, ast::Expression)| {
            if op == "AND" {
                ast::Expression::And(Box::new(acc), Box::new(val))
            } else {
                ast::Expression::Or(Box::new(acc), Box::new(val))
            }
        },
    )(i)
}

fn expression<'a>(i: &'a str) -> IResult<&'a str, ast::Expression, VerboseError<&'a str>> {
    alt((map(condition, ast::Expression::Condition), expression_term))(i)
}

fn select_expression<'a>(i: &'a str) -> IResult<&'a str, ast::SelectExpression, VerboseError<&'a str>> {
    preceded(
        space0,
        alt((
            map(char('*'), |_| ast::SelectExpression::Star),
            map(
                pair(
                    expression,
                    opt(preceded(tuple((space1, tag("as"), space1)), identifier)),
                ),
                |(e, name_opt)| ast::SelectExpression::Expression(Box::new(e), name_opt.map(|s| s.to_string())),
            ),
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
    map(preceded(tag("where"), expression), ast::WhereExpression::new)(i)
}

fn column_expression_list<'a>(i: &'a str) -> IResult<&'a str, Vec<ast::ColumnName>, VerboseError<&'a str>> {
    context(
        "column_expression_list",
        map(
            terminated(separated_list(preceded(space0, char(',')), column_name), space0),
            |v| v.into_iter().map(|s| s.to_string()).collect(),
        ),
    )(i)
}

fn group_by_expression<'a>(i: &'a str) -> IResult<&'a str, ast::GroupByExpression, VerboseError<&'a str>> {
    map(
        preceded(tuple((tag("group"), space1, tag("by"), space1)), column_expression_list),
        ast::GroupByExpression::new,
    )(i)
}

fn limit_expression<'a>(i: &'a str) -> IResult<&'a str, ast::LimitExpression, VerboseError<&'a str>> {
    map(preceded(tuple((tag("limit"), space1)), digit1), |s: &str| {
        let v = s.parse::<u32>().unwrap();
        ast::LimitExpression::new(v)
    })(i)
}

fn ordering_term<'a>(i: &'a str) -> IResult<&'a str, ast::OrderingTerm, VerboseError<&'a str>> {
    map(
        pair(column_name, preceded(space1, alt((tag("asc"), tag("desc"))))),
        |(column_name, ordering)| ast::OrderingTerm::new(column_name, ordering),
    )(i)
}

fn order_by_clause<'a>(i: &'a str) -> IResult<&'a str, ast::OrderByExpression, VerboseError<&'a str>> {
    map(
        preceded(
            tuple((tag("order"), space1, tag("by"), space1)),
            terminated(separated_list(preceded(space0, char(',')), ordering_term), space0),
        ),
        ast::OrderByExpression::new,
    )(i)
}

fn from_clause<'a>(i: &'a str) -> IResult<&'a str, &'a str, VerboseError<&'a str>> {
    terminated(preceded(tuple((tag("from"), space1)), identifier), space0)(i)
}

pub(crate) fn select_query<'a>(i: &'a str) -> IResult<&'a str, ast::SelectStatement, VerboseError<&'a str>> {
    map(
        preceded(
            tag("select"),
            tuple((
                select_expression_list,
                from_clause,
                opt(where_expression),
                opt(group_by_expression),
                opt(order_by_clause),
                opt(limit_expression),
            )),
        ),
        |(select_exprs, table_name, where_expr, group_by_expr, order_by_expr, limit_expr)| {
            ast::SelectStatement::new(
                select_exprs,
                table_name,
                where_expr,
                group_by_expr,
                order_by_expr,
                limit_expr,
            )
        },
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
        assert_eq!(
            string_literal("\"abc\""),
            Ok(("", ast::Value::StringLiteral("abc".to_string())))
        );
        assert_eq!(
            string_literal("\"def\""),
            Ok(("", ast::Value::StringLiteral("def".to_string())))
        );
        assert_eq!(
            string_literal("\"10.0.2.143:80\""),
            Ok(("", ast::Value::StringLiteral("10.0.2.143:80".to_string())))
        );
        assert_eq!(
            string_literal("\"10.0\n5,3|\""),
            Ok(("", ast::Value::StringLiteral("10.0\n5,3|".to_string())))
        );
    }

    #[test]
    fn test_integral() {
        assert_eq!(integral("123"), Ok(("", ast::Value::Integral(123))));
        assert_eq!(integral("-123"), Ok(("", ast::Value::Integral(-123))));
    }

    #[test]
    fn test_where_value_expression() {
        let ans = ast::ValueExpression::Operator(
            ast::ValueOperator::Plus,
            Box::new(ast::ValueExpression::Operator(
                ast::ValueOperator::Plus,
                Box::new(ast::ValueExpression::Value(ast::Value::Integral(1))),
                Box::new(ast::ValueExpression::Value(ast::Value::Integral(2))),
            )),
            Box::new(ast::ValueExpression::Value(ast::Value::Integral(3))),
        );
        assert_eq!(value_expression("1 + 2 + 3"), Ok(("", ans)));
    }

    #[test]
    fn test_condition_expression() {
        let ans = ast::Condition::ComparisonExpression(
            ast::RelationOperator::Equal,
            Box::new(ast::ValueExpression::Operator(
                ast::ValueOperator::Plus,
                Box::new(ast::ValueExpression::Value(ast::Value::Integral(1))),
                Box::new(ast::ValueExpression::Value(ast::Value::Integral(2))),
            )),
            Box::new(ast::ValueExpression::Value(ast::Value::Integral(3))),
        );

        assert_eq!(condition("1 + 2 = 3"), Ok(("", ans)));
    }

    #[test]
    fn test_expression_term() {
        let ans = ast::Expression::And(
            Box::new(ast::Expression::Value(Box::new(ast::ValueExpression::Value(
                ast::Value::Boolean(true),
            )))),
            Box::new(ast::Expression::Not(Box::new(ast::Expression::Value(Box::new(
                ast::ValueExpression::Value(ast::Value::Boolean(true)),
            ))))),
        );
        assert_eq!(expression_term("true AND NOT true"), Ok(("", ans)));
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
            ast::SelectExpression::Expression(
                Box::new(ast::Expression::Value(Box::new(ast::ValueExpression::Value(
                    ast::Value::Integral(1),
                )))),
                None,
            ),
            ast::SelectExpression::Expression(
                Box::new(ast::Expression::Value(Box::new(ast::ValueExpression::Value(
                    ast::Value::Integral(2),
                )))),
                None,
            ),
            ast::SelectExpression::Expression(
                Box::new(ast::Expression::Value(Box::new(ast::ValueExpression::Value(
                    ast::Value::Integral(3),
                )))),
                None,
            ),
        ];
        assert_eq!(select_expression_list("1, 2, 3"), Ok(("", ans)));

        let ans = vec![
            ast::SelectExpression::Expression(
                Box::new(ast::Expression::Value(Box::new(ast::ValueExpression::Column(
                    "a".to_string(),
                )))),
                None,
            ),
            ast::SelectExpression::Expression(
                Box::new(ast::Expression::Value(Box::new(ast::ValueExpression::Column(
                    "b".to_string(),
                )))),
                None,
            ),
            ast::SelectExpression::Expression(
                Box::new(ast::Expression::Value(Box::new(ast::ValueExpression::Column(
                    "c".to_string(),
                )))),
                None,
            ),
        ];
        assert_eq!(select_expression_list("a, b, c"), Ok(("", ans)));
    }

    #[test]
    fn test_select_statement() {
        let select_exprs = vec![
            ast::SelectExpression::Expression(
                Box::new(ast::Expression::Value(Box::new(ast::ValueExpression::Column(
                    "a".to_string(),
                )))),
                None,
            ),
            ast::SelectExpression::Expression(
                Box::new(ast::Expression::Value(Box::new(ast::ValueExpression::Column(
                    "b".to_string(),
                )))),
                None,
            ),
            ast::SelectExpression::Expression(
                Box::new(ast::Expression::Value(Box::new(ast::ValueExpression::Column(
                    "c".to_string(),
                )))),
                None,
            ),
        ];

        let where_expr = ast::WhereExpression::new(ast::Expression::Condition(ast::Condition::ComparisonExpression(
            ast::RelationOperator::Equal,
            Box::new(ast::ValueExpression::Column("a".to_string())),
            Box::new(ast::ValueExpression::Value(ast::Value::Integral(1))),
        )));
        let ans = ast::SelectStatement::new(select_exprs, "elb", Some(where_expr), None, None, None);

        assert_eq!(select_query("select a, b, c from elb where a = 1"), Ok(("", ans)));
        let select_exprs = vec![
            ast::SelectExpression::Expression(
                Box::new(ast::Expression::Value(Box::new(ast::ValueExpression::Column(
                    "a".to_string(),
                )))),
                None,
            ),
            ast::SelectExpression::Expression(
                Box::new(ast::Expression::Value(Box::new(ast::ValueExpression::Column(
                    "b".to_string(),
                )))),
                None,
            ),
            ast::SelectExpression::Expression(
                Box::new(ast::Expression::Value(Box::new(ast::ValueExpression::Column(
                    "c".to_string(),
                )))),
                None,
            ),
        ];

        let where_expr = ast::WhereExpression::new(ast::Expression::Condition(ast::Condition::ComparisonExpression(
            ast::RelationOperator::Equal,
            Box::new(ast::ValueExpression::Column("a".to_string())),
            Box::new(ast::ValueExpression::Value(ast::Value::Integral(1))),
        )));

        let group_by_expr = ast::GroupByExpression::new(vec!["b".to_string()]);
        let ans = ast::SelectStatement::new(select_exprs, "elb", Some(where_expr), Some(group_by_expr), None, None);

        assert_eq!(
            select_query("select a, b, c from elb where a = 1 group by b"),
            Ok(("", ans))
        );
    }

    #[test]
    fn test_select_statement_with_func_call() {
        let select_exprs = vec![
            ast::SelectExpression::Expression(
                Box::new(ast::Expression::Value(Box::new(ast::ValueExpression::FuncCall(
                    "avg".to_string(),
                    vec![ast::SelectExpression::Expression(
                        Box::new(ast::Expression::Value(Box::new(ast::ValueExpression::Column(
                            "a".to_string(),
                        )))),
                        None,
                    )],
                    None,
                )))),
                None,
            ),
            ast::SelectExpression::Expression(
                Box::new(ast::Expression::Value(Box::new(ast::ValueExpression::Column(
                    "b".to_string(),
                )))),
                None,
            ),
            ast::SelectExpression::Expression(
                Box::new(ast::Expression::Value(Box::new(ast::ValueExpression::Column(
                    "c".to_string(),
                )))),
                None,
            ),
        ];

        let where_expr = ast::WhereExpression::new(ast::Expression::Condition(ast::Condition::ComparisonExpression(
            ast::RelationOperator::Equal,
            Box::new(ast::ValueExpression::Column("a".to_string())),
            Box::new(ast::ValueExpression::Value(ast::Value::Integral(1))),
        )));
        let ans = ast::SelectStatement::new(select_exprs, "elb", Some(where_expr), None, None, None);

        assert_eq!(select_query("select avg(a), b, c from elb where a = 1"), Ok(("", ans)));
    }

    #[test]
    fn test_select_statement_with_limit() {
        let select_exprs = vec![
            ast::SelectExpression::Expression(
                Box::new(ast::Expression::Value(Box::new(ast::ValueExpression::Column(
                    "a".to_string(),
                )))),
                None,
            ),
            ast::SelectExpression::Expression(
                Box::new(ast::Expression::Value(Box::new(ast::ValueExpression::Column(
                    "b".to_string(),
                )))),
                None,
            ),
            ast::SelectExpression::Expression(
                Box::new(ast::Expression::Value(Box::new(ast::ValueExpression::Column(
                    "c".to_string(),
                )))),
                None,
            ),
        ];

        let limit_expr = ast::LimitExpression::new(1);
        let ans = ast::SelectStatement::new(select_exprs, "elb", None, None, None, Some(limit_expr));

        assert_eq!(select_query("select a, b, c from elb limit 1"), Ok(("", ans)));
    }

    #[test]
    fn test_select_statement_with_order() {
        let select_exprs = vec![
            ast::SelectExpression::Expression(
                Box::new(ast::Expression::Value(Box::new(ast::ValueExpression::Column(
                    "a".to_string(),
                )))),
                None,
            ),
            ast::SelectExpression::Expression(
                Box::new(ast::Expression::Value(Box::new(ast::ValueExpression::Column(
                    "b".to_string(),
                )))),
                None,
            ),
            ast::SelectExpression::Expression(
                Box::new(ast::Expression::Value(Box::new(ast::ValueExpression::Column(
                    "c".to_string(),
                )))),
                None,
            ),
        ];

        let order_by_clause = ast::OrderByExpression::new(vec![ast::OrderingTerm::new("a", "asc")]);
        let ans = ast::SelectStatement::new(select_exprs, "elb", None, None, Some(order_by_clause), None);

        assert_eq!(select_query("select a, b, c from elb order by a asc"), Ok(("", ans)));
    }

    #[test]
    fn test_select_statement_with_within_group() {
        let select_exprs = vec![
            ast::SelectExpression::Expression(
                Box::new(ast::Expression::Value(Box::new(ast::ValueExpression::Column(
                    "a".to_string(),
                )))),
                None,
            ),
            ast::SelectExpression::Expression(
                Box::new(ast::Expression::Value(Box::new(ast::ValueExpression::FuncCall(
                    "percentile_disc".to_string(),
                    vec![ast::SelectExpression::Expression(
                        Box::new(ast::Expression::Value(Box::new(ast::ValueExpression::Value(
                            ast::Value::Boolean(true),
                        )))),
                        None,
                    )],
                    Some(ast::WithinGroupClause::new(ast::OrderByExpression::new(vec![
                        ast::OrderingTerm::new("b", "asc"),
                    ]))),
                )))),
                None,
            ),
        ];

        let group_by_expr = ast::GroupByExpression::new(vec!["a".to_string()]);
        let ans = ast::SelectStatement::new(select_exprs, "elb", None, Some(group_by_expr), None, None);

        assert_eq!(
            select_query("select a, percentile_disc(true) within group (order by b asc) from elb group by a"),
            Ok(("", ans))
        );
    }
}
