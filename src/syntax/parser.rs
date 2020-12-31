use super::ast;
use ordered_float::OrderedFloat;
use std::str::FromStr;

use nom::{
    branch::alt,
    bytes::complete::{escaped, tag},
    character::complete::{char, digit1, multispace0, none_of, one_of, space0, space1},
    combinator::{cut, map, map_res, not, opt},
    error::{context, VerboseError},
    multi::separated_list0,
    number::complete,
    sequence::{delimited, pair, preceded, terminated, tuple},
    AsChar, IResult, InputTakeAtPosition,
};

use crate::syntax::ast::{ArrayConstructor, PathExpr, PathSegment, TableReference, TupleConstructor};
use hashbrown::hash_map::HashMap;

lazy_static! {
    static ref KEYWORDS: Vec<&'static str> = {
        vec![
            "select", "value", "from", "where", "group", "by", "limit", "order", "true", "false", "case", "when",
            "then",
        ]
    };
}

fn case_when_expression(i: &str) -> IResult<&str, ast::CaseWhenExpression, VerboseError<&str>> {
    let (remaining_input, (_, condition, _, then_expr, else_expr, _)) = tuple((
        tuple((tag("case"), space1, tag("when"), space1)),
        expression,
        tuple((multispace0, tag("then"), space1)),
        factor,
        opt(delimited(terminated(tag("else"), space1), factor, multispace0)),
        tag("end"),
    ))(i)?;

    Ok((
        remaining_input,
        ast::CaseWhenExpression {
            condition: Box::new(condition),
            then_expr: Box::new(then_expr),
            else_expr: else_expr.map(|n| Box::new(n)),
        },
    ))
}

fn double_quote_string_literal_interior(i: &str) -> IResult<&str, &str, VerboseError<&str>> {
    escaped(none_of("\""), '\\', one_of("\"n\\"))(i)
}

fn double_quote_string_literal(i: &str) -> IResult<&str, ast::Value, VerboseError<&str>> {
    context(
        "string",
        map(
            preceded(
                char('\"'),
                cut(terminated(double_quote_string_literal_interior, char('\"'))),
            ),
            |s| ast::Value::StringLiteral(s.to_string()),
        ),
    )(i)
}

fn single_quote_string_literal_interior(i: &str) -> IResult<&str, &str, VerboseError<&str>> {
    escaped(none_of("'"), '\\', one_of("\"n\\"))(i)
}

fn single_quote_string_literal(i: &str) -> IResult<&str, String, VerboseError<&str>> {
    context(
        "single_quote_string_literal",
        map(
            preceded(
                char('\''),
                cut(terminated(single_quote_string_literal_interior, char('\''))),
            ),
            |s| s.to_string(),
        ),
    )(i)
}

fn identifier<'a, E: nom::error::ParseError<&'a str>>(input: &'a str) -> IResult<&'a str, &'a str, E>
where
{
    fn is_alphanumeric_or_underscore(chr: char) -> bool {
        chr.is_alphanumeric() || chr == '_'
    }

    fn start_with_number(s: &str) -> bool {
        if let Some(c) = s.chars().next() {
            "0123456789".contains(c)
        } else {
            false
        }
    }

    fn all_underscore(s: &str) -> bool {
        for c in s.chars() {
            if c != '_' {
                return false;
            }
        }

        true
    }

    let result = input.split_at_position1_complete(
        |item| !is_alphanumeric_or_underscore(item.as_char()),
        nom::error::ErrorKind::Alpha,
    );

    match result {
        Ok((i, o)) => {
            if start_with_number(o) || all_underscore(o) || KEYWORDS.contains(&o) {
                Err(nom::Err::Failure(nom::error::ParseError::from_error_kind(
                    i,
                    nom::error::ErrorKind::Alpha, //FIXME: customize error
                )))
            } else {
                Ok((i, o))
            }
        }
        Err(e) => Err(e),
    }
}

fn column_name(i: &str) -> IResult<&str, &str, VerboseError<&str>> {
    terminated(identifier, not(char('(')))(i)
}

fn boolean(i: &str) -> IResult<&str, ast::Value, VerboseError<&str>> {
    alt((
        map(tag("true"), |_| ast::Value::Boolean(true)),
        map(tag("false"), |_| ast::Value::Boolean(false)),
    ))(i)
}

fn float(i: &str) -> IResult<&str, ast::Value, VerboseError<&str>> {
    map(complete::float, |f| ast::Value::Float(OrderedFloat::from(f)))(i)
}

fn integral(i: &str) -> IResult<&str, ast::Value, VerboseError<&str>> {
    alt((
        map_res(terminated(digit1, not(char('.'))), |digit_str: &str| {
            digit_str.parse::<i32>().map(ast::Value::Integral)
        }),
        map(
            terminated(preceded(tag("-"), digit1), not(char('.'))),
            |digit_str: &str| ast::Value::Integral(-digit_str.parse::<i32>().unwrap()),
        ),
    ))(i)
}

fn value(i: &str) -> IResult<&str, ast::Value, VerboseError<&str>> {
    alt((integral, float, boolean, double_quote_string_literal))(i)
}

fn parens(i: &str) -> IResult<&str, ast::Expression, VerboseError<&str>> {
    delimited(space0, delimited(tag("("), expression, tag(")")), space0)(i)
}

fn order_by_clause_for_within_group(i: &str) -> IResult<&str, ast::OrderByExpression, VerboseError<&str>> {
    map(
        preceded(tuple((tag("order"), space1, tag("by"), space1)), ordering_term),
        |item| ast::OrderByExpression::new(vec![item]),
    )(i)
}

fn within_group_clause(i: &str) -> IResult<&str, ast::WithinGroupClause, VerboseError<&str>> {
    map(
        preceded(
            tuple((space1, tag("within"), space1, tag("group"), space1)),
            delimited(tag("("), order_by_clause_for_within_group, tag(")")),
        ),
        ast::WithinGroupClause::new,
    )(i)
}

fn func_call(i: &str) -> IResult<&str, ast::Expression, VerboseError<&str>> {
    map(
        tuple((
            identifier,
            delimited(tag("("), opt(select_expression_list), tag(")")),
            opt(within_group_clause),
        )),
        |(func_name, select_expr_list_opt, within_group_opt)| {
            if let Some(select_expr_list) = select_expr_list_opt {
                ast::Expression::FuncCall(func_name.to_string(), select_expr_list, within_group_opt)
            } else {
                ast::Expression::FuncCall(func_name.to_string(), vec![], within_group_opt)
            }
        },
    )(i)
}

fn select_column_reference(i: &str) -> IResult<&str, ast::PathExpr, VerboseError<&str>> {
    terminated(path_expr, not(char('(')))(i)
}

fn factor(i: &str) -> IResult<&str, ast::Expression, VerboseError<&str>> {
    delimited(
        space0,
        alt((
            parens,
            map(value, ast::Expression::Value),
            func_call,
            map(select_column_reference, |path_expr| ast::Expression::Column(path_expr)),
        )),
        space0,
    )(i)
}

fn expression_term_opt_not(i: &str) -> IResult<&str, ast::Expression, VerboseError<&str>> {
    map(
        pair(opt(tuple((space1, tag("not"), space1))), factor),
        |(opt_not, factor)| {
            if opt_not.is_some() {
                ast::Expression::UnaryOperator(ast::UnaryOperator::Not, Box::new(factor))
            } else {
                factor
            }
        },
    )(i)
}

fn expression(i: &str) -> IResult<&str, ast::Expression, VerboseError<&str>> {
    let mut precedence_table: HashMap<String, (u32, bool)> = HashMap::new();
    precedence_table.insert("*".to_string(), (7, true));
    precedence_table.insert("/".to_string(), (7, true));
    precedence_table.insert("+".to_string(), (6, true));
    precedence_table.insert("-".to_string(), (6, true));
    precedence_table.insert("<".to_string(), (4, true));
    precedence_table.insert("<=".to_string(), (4, true));
    precedence_table.insert(">".to_string(), (4, true));
    precedence_table.insert(">=".to_string(), (4, true));
    precedence_table.insert("=".to_string(), (3, true));
    precedence_table.insert("!=".to_string(), (3, true));
    precedence_table.insert("and".to_string(), (2, true));
    precedence_table.insert("or".to_string(), (1, true));

    let (i1, expr) = parse_expression_at_precedence(i, 1, &precedence_table)?;
    Ok((i1, expr))
}

fn select_expression(i: &str) -> IResult<&str, ast::SelectExpression, VerboseError<&str>> {
    preceded(
        space0,
        alt((
            map(char('*'), |_| ast::SelectExpression::Star),
            map(
                pair(
                    expression,
                    opt(preceded(tuple((space0, tag("as"), space1)), identifier)),
                ),
                |(e, name_opt)| ast::SelectExpression::Expression(Box::new(e), name_opt.map(|s| s.to_string())),
            ),
        )),
    )(i)
}

fn select_expression_list(i: &str) -> IResult<&str, Vec<ast::SelectExpression>, VerboseError<&str>> {
    context(
        "select_expression_list",
        terminated(separated_list0(preceded(space0, char(',')), select_expression), space0),
    )(i)
}

fn where_expression(i: &str) -> IResult<&str, ast::WhereExpression, VerboseError<&str>> {
    map(preceded(tag("where"), expression), ast::WhereExpression::new)(i)
}

fn qualified_column_name(i: &str) -> IResult<&str, Vec<String>, VerboseError<&str>> {
    map(terminated(separated_list0(char('.'), identifier), space0), |v| {
        v.iter().map(|s| s.to_string()).collect()
    })(i)
}

fn column_name_in_group_by(i: &str) -> IResult<&str, Vec<String>, VerboseError<&str>> {
    terminated(qualified_column_name, not(char('(')))(i)
}

fn column_reference(i: &str) -> IResult<&str, ast::GroupByReference, VerboseError<&str>> {
    map(
        tuple((
            column_name_in_group_by,
            opt(preceded(tuple((space0, tag("as"), space1)), identifier)),
        )),
        |(column_name, as_clasue)| ast::GroupByReference::new(column_name, as_clasue.map(|s| s.to_string())),
    )(i)
}

fn column_expression_list(i: &str) -> IResult<&str, Vec<ast::GroupByReference>, VerboseError<&str>> {
    context(
        "column_expression_list",
        map(
            terminated(
                separated_list0(preceded(space0, char(',')), preceded(space0, column_reference)),
                space0,
            ),
            |v| v.into_iter().collect(),
        ),
    )(i)
}

fn path_bracket(i: &str) -> IResult<&str, ast::Value, VerboseError<&str>> {
    delimited(tag("["), integral, tag("]"))(i)
}

fn path_expr(i: &str) -> IResult<&str, PathExpr, VerboseError<&str>> {
    map(
        terminated(separated_list0(char('.'), pair(identifier, opt(path_bracket))), space0),
        |v| {
            let segments = v
                .iter()
                .map(|(attr_name, opt_array_idx)| {
                    if let Some(array_idx) = opt_array_idx {
                        match array_idx {
                            ast::Value::Integral(i) => PathSegment::ArrayIndex(attr_name.to_string(), *i as usize),
                            _ => {
                                unreachable!()
                            }
                        }
                    } else {
                        PathSegment::AttrName(attr_name.to_string())
                    }
                })
                .collect();
            PathExpr::new(segments)
        },
    )(i)
}

fn table_reference(i: &str) -> IResult<&str, ast::TableReference, VerboseError<&str>> {
    map(
        tuple((
            path_expr,
            opt(preceded(tuple((space0, tag("as"), space1)), identifier)),
            opt(preceded(tuple((space0, tag("at"), space1)), identifier)),
        )),
        |(path_expr, as_clasue, at_clause)| {
            ast::TableReference::new(
                path_expr,
                as_clasue.map(|s| s.to_string()),
                at_clause.map(|s| s.to_string()),
            )
        },
    )(i)
}

fn table_reference_list(i: &str) -> IResult<&str, Vec<ast::TableReference>, VerboseError<&str>> {
    context(
        "table_reference_list",
        terminated(
            separated_list0(preceded(space0, char(',')), preceded(space0, table_reference)),
            space0,
        ),
    )(i)
}

fn having_expression(i: &str) -> IResult<&str, ast::WhereExpression, VerboseError<&str>> {
    map(preceded(tag("having"), expression), ast::WhereExpression::new)(i)
}

fn group_by_expression(i: &str) -> IResult<&str, ast::GroupByExpression, VerboseError<&str>> {
    map(
        preceded(
            tuple((tag("group"), space1, tag("by"), space1)),
            pair(
                column_expression_list,
                opt(preceded(
                    tuple((space0, tag("group"), space1, tag("as"), space1)),
                    identifier,
                )),
            ),
        ),
        |(group_by_references, group_as_clause)| {
            ast::GroupByExpression::new(group_by_references, group_as_clause.map(|s| s.to_string()))
        },
    )(i)
}

fn limit_expression(i: &str) -> IResult<&str, ast::LimitExpression, VerboseError<&str>> {
    map(preceded(tuple((tag("limit"), space1)), digit1), |s: &str| {
        let v = s.parse::<u32>().unwrap();
        ast::LimitExpression::new(v)
    })(i)
}

fn ordering_term(i: &str) -> IResult<&str, ast::OrderingTerm, VerboseError<&str>> {
    map(
        pair(column_name, preceded(space1, alt((tag("asc"), tag("desc"))))),
        |(column_name, ordering)| ast::OrderingTerm::new(column_name, ordering),
    )(i)
}

fn order_by_clause(i: &str) -> IResult<&str, ast::OrderByExpression, VerboseError<&str>> {
    map(
        preceded(
            tuple((tag("order"), space1, tag("by"), space1)),
            terminated(separated_list0(preceded(space0, char(',')), ordering_term), space0),
        ),
        ast::OrderByExpression::new,
    )(i)
}

fn from_clause(i: &str) -> IResult<&str, Vec<TableReference>, VerboseError<&str>> {
    terminated(preceded(tuple((tag("from"), space1)), table_reference_list), space0)(i)
}

fn parse_expression_atom(i: &str) -> IResult<&str, ast::Expression, VerboseError<&str>> {
    alt((
        map(case_when_expression, |n| ast::Expression::CaseWhenExpression(n)),
        expression_term_opt_not,
    ))(i)
}

fn parse_expression_op(i: &str) -> IResult<&str, &str, VerboseError<&str>> {
    alt((
        tag("+"),
        tag("-"),
        tag("*"),
        tag("/"),
        tag("="),
        tag("!="),
        tag(">"),
        tag("<"),
        tag(">="),
        tag("<="),
        tag("and"),
        tag("or"),
    ))(i)
}

fn parse_expression_at_precedence<'a>(
    i0: &'a str,
    current_precedence: u32,
    precedence_table: &HashMap<String, (u32, bool)>,
) -> IResult<&'a str, ast::Expression, VerboseError<&'a str>> {
    let (mut i1, mut expr) = parse_expression_atom(i0)?;
    while let Ok((i2, op)) = parse_expression_op(i1) {
        let (op_precedence, op_left_associative) = *precedence_table.get(op).unwrap();

        if op_precedence < current_precedence {
            break;
        }

        let (i3, b) = if op_left_associative {
            parse_expression_at_precedence(i2, op_precedence + 1, precedence_table)?
        } else {
            parse_expression_at_precedence(i2, op_precedence, precedence_table)?
        };

        let op = ast::BinaryOperator::from_str(op).unwrap();
        expr = ast::Expression::BinaryOperator(op, Box::new(expr), Box::new(b));

        i1 = i3;
    }

    Ok((i1, expr))
}

fn tuple_constructor_expression_term(i: &str) -> IResult<&str, (String, ast::Expression), VerboseError<&str>> {
    context(
        "tuple_constructor_expression_term",
        map(
            delimited(
                space0,
                tuple((single_quote_string_literal, tag(":"), expression)),
                space0,
            ),
            |(first, _, second)| (first, second),
        ),
    )(i)
}

fn tuple_constructor_expression_list(i: &str) -> IResult<&str, Vec<(String, ast::Expression)>, VerboseError<&str>> {
    context(
        "tuple_constructor_expression_list",
        terminated(
            separated_list0(preceded(space0, char(',')), tuple_constructor_expression_term),
            space0,
        ),
    )(i)
}

fn tuple_constructor(i: &str) -> IResult<&str, ast::TupleConstructor, VerboseError<&str>> {
    map(
        delimited(
            space0,
            delimited(tag("{"), tuple_constructor_expression_list, tag("}")),
            space0,
        ),
        |v| TupleConstructor { key_values: v },
    )(i)
}

fn array_constructor_expression_list(i: &str) -> IResult<&str, Vec<ast::Expression>, VerboseError<&str>> {
    context(
        "array_constructor_expression_list",
        terminated(separated_list0(preceded(space0, char(',')), expression), space0),
    )(i)
}

fn array_constructor(i: &str) -> IResult<&str, ast::ArrayConstructor, VerboseError<&str>> {
    map(
        delimited(
            space0,
            delimited(tag("["), array_constructor_expression_list, tag("]")),
            space0,
        ),
        |v| ArrayConstructor { values: v },
    )(i)
}

pub(crate) fn select_query(i: &str) -> IResult<&str, ast::SelectStatement, VerboseError<&str>> {
    map(
        preceded(
            tag("select"),
            tuple((
                map(opt(delimited(space1, tag("value"), space1)), |x| x.is_some()),
                select_expression_list,
                from_clause,
                opt(where_expression),
                opt(group_by_expression),
                opt(having_expression),
                opt(order_by_clause),
                opt(limit_expression),
            )),
        ),
        |(
            is_select_value,
            select_exprs,
            table_references,
            where_expr,
            group_by_expr,
            having_expr,
            order_by_expr,
            limit_expr,
        )| {
            ast::SelectStatement::new(
                is_select_value,
                select_exprs,
                table_references,
                where_expr,
                group_by_expr,
                having_expr,
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
    fn test_identifier() {
        assert_eq!(
            identifier::<VerboseError<&str>>("true"),
            Err(nom::Err::Failure(VerboseError {
                errors: vec![("", nom::error::VerboseErrorKind::Nom(nom::error::ErrorKind::Alpha))]
            }))
        );
        assert_eq!(
            identifier::<VerboseError<&str>>("false"),
            Err(nom::Err::Failure(VerboseError {
                errors: vec![("", nom::error::VerboseErrorKind::Nom(nom::error::ErrorKind::Alpha))]
            }))
        );
        assert_eq!(
            identifier::<VerboseError<&str>>("select"),
            Err(nom::Err::Failure(VerboseError {
                errors: vec![("", nom::error::VerboseErrorKind::Nom(nom::error::ErrorKind::Alpha))]
            }))
        );
        assert_eq!(
            identifier::<VerboseError<&str>>("order"),
            Err(nom::Err::Failure(VerboseError {
                errors: vec![("", nom::error::VerboseErrorKind::Nom(nom::error::ErrorKind::Alpha))]
            }))
        );
        assert_eq!(
            identifier::<VerboseError<&str>>("____"),
            Err(nom::Err::Failure(VerboseError {
                errors: vec![("", nom::error::VerboseErrorKind::Nom(nom::error::ErrorKind::Alpha))]
            }))
        );
        assert_eq!(
            identifier::<VerboseError<&str>>("123abc"),
            Err(nom::Err::Failure(VerboseError {
                errors: vec![("", nom::error::VerboseErrorKind::Nom(nom::error::ErrorKind::Alpha))]
            }))
        );
        assert_eq!(identifier::<VerboseError<&str>>("abc_fef"), Ok(("", "abc_fef")));
    }

    #[test]
    fn test_boolean() {
        assert_eq!(boolean("true"), Ok(("", ast::Value::Boolean(true))));
        assert_eq!(boolean("false"), Ok(("", ast::Value::Boolean(false))));
    }

    #[test]
    fn test_string_literal() {
        assert_eq!(
            double_quote_string_literal("\"abc\""),
            Ok(("", ast::Value::StringLiteral("abc".to_string())))
        );
        assert_eq!(
            double_quote_string_literal("\"def\""),
            Ok(("", ast::Value::StringLiteral("def".to_string())))
        );
        assert_eq!(
            double_quote_string_literal("\"10.0.2.143:80\""),
            Ok(("", ast::Value::StringLiteral("10.0.2.143:80".to_string())))
        );
        assert_eq!(
            double_quote_string_literal("\"10.0\n5,3|\""),
            Ok(("", ast::Value::StringLiteral("10.0\n5,3|".to_string())))
        );
    }

    #[test]
    fn test_integral() {
        assert_eq!(integral("123"), Ok(("", ast::Value::Integral(123))));
        assert_eq!(integral("-123"), Ok(("", ast::Value::Integral(-123))));
    }

    #[test]
    fn test_float() {
        assert_eq!(float("123.0"), Ok(("", ast::Value::Float(OrderedFloat::from(123.0)))));
        assert_eq!(
            float("-123.123"),
            Ok(("", ast::Value::Float(OrderedFloat::from(-123.123))))
        );
        assert_eq!(float("0.9"), Ok(("", ast::Value::Float(OrderedFloat::from(0.9)))));
    }

    #[test]
    fn test_case_when_expression() {
        let ans = ast::CaseWhenExpression {
            condition: Box::new(ast::Expression::Value(ast::Value::Boolean(true))),
            then_expr: Box::new(ast::Expression::Value(ast::Value::Integral(1))),
            else_expr: Some(Box::new(ast::Expression::Value(ast::Value::Integral(2)))),
        };

        assert_eq!(case_when_expression("case when true then 1 else 2 end"), Ok(("", ans)));
    }

    #[test]
    fn test_condition_expression() {
        let path_expr_a = PathExpr::new(vec![PathSegment::AttrName("a".to_string())]);
        let path_expr_b = PathExpr::new(vec![PathSegment::AttrName("b".to_string())]);

        let ans = ast::Expression::BinaryOperator(
            ast::BinaryOperator::Equal,
            Box::new(ast::Expression::BinaryOperator(
                ast::BinaryOperator::Plus,
                Box::new(ast::Expression::Column(path_expr_a.clone())),
                Box::new(ast::Expression::Column(path_expr_b.clone())),
            )),
            Box::new(ast::Expression::Value(ast::Value::Integral(3))),
        );

        assert_eq!(expression("a+b = 3"), Ok(("", ans)));

        let ans = ast::Expression::BinaryOperator(
            ast::BinaryOperator::MoreThan,
            Box::new(ast::Expression::BinaryOperator(
                ast::BinaryOperator::Plus,
                Box::new(ast::Expression::Value(ast::Value::Integral(1))),
                Box::new(ast::Expression::Value(ast::Value::Integral(3))),
            )),
            Box::new(ast::Expression::Value(ast::Value::Integral(3))),
        );

        assert_eq!(expression("1 + 3 > 3"), Ok(("", ans)));

        let ans = ast::Expression::BinaryOperator(
            ast::BinaryOperator::NotEqual,
            Box::new(ast::Expression::BinaryOperator(
                ast::BinaryOperator::Minus,
                Box::new(ast::Expression::Value(ast::Value::Integral(1))),
                Box::new(ast::Expression::Value(ast::Value::Integral(4))),
            )),
            Box::new(ast::Expression::Value(ast::Value::Integral(3))),
        );

        assert_eq!(expression("1-4 !=3"), Ok(("", ans)));
    }

    #[test]
    fn test_value_expression() {
        let ans = ast::Expression::BinaryOperator(
            ast::BinaryOperator::Plus,
            Box::new(ast::Expression::BinaryOperator(
                ast::BinaryOperator::Plus,
                Box::new(ast::Expression::Value(ast::Value::Integral(1))),
                Box::new(ast::Expression::Value(ast::Value::Integral(2))),
            )),
            Box::new(ast::Expression::Value(ast::Value::Integral(3))),
        );
        assert_eq!(expression("1 + 2 + 3"), Ok(("", ans)));

        let ans = ast::Expression::BinaryOperator(
            ast::BinaryOperator::Minus,
            Box::new(ast::Expression::BinaryOperator(
                ast::BinaryOperator::Plus,
                Box::new(ast::Expression::BinaryOperator(
                    ast::BinaryOperator::Times,
                    Box::new(ast::Expression::Value(ast::Value::Integral(3))),
                    Box::new(ast::Expression::Value(ast::Value::Integral(1))),
                )),
                Box::new(ast::Expression::BinaryOperator(
                    ast::BinaryOperator::Divide,
                    Box::new(ast::Expression::Value(ast::Value::Integral(3))),
                    Box::new(ast::Expression::Value(ast::Value::Integral(1))),
                )),
            )),
            Box::new(ast::Expression::Value(ast::Value::Integral(5))),
        );

        assert_eq!(expression("3*1 + 3/1 - (5)"), Ok(("", ans)));
    }

    #[test]
    fn test_expression_term() {
        let path_expr_a = PathExpr::new(vec![PathSegment::AttrName("a".to_string())]);
        let path_expr_b = PathExpr::new(vec![PathSegment::AttrName("b".to_string())]);

        let ans = ast::Expression::BinaryOperator(
            ast::BinaryOperator::And,
            Box::new(ast::Expression::Value(ast::Value::Boolean(true))),
            Box::new(ast::Expression::UnaryOperator(
                ast::UnaryOperator::Not,
                Box::new(ast::Expression::Value(ast::Value::Boolean(true))),
            )),
        );
        assert_eq!(expression("true and not true"), Ok(("", ans)));

        let ans = ast::Expression::BinaryOperator(
            ast::BinaryOperator::And,
            Box::new(ast::Expression::BinaryOperator(
                ast::BinaryOperator::Equal,
                Box::new(ast::Expression::Column(path_expr_a.clone())),
                Box::new(ast::Expression::Value(ast::Value::Integral(1))),
            )),
            Box::new(ast::Expression::BinaryOperator(
                ast::BinaryOperator::Equal,
                Box::new(ast::Expression::Column(path_expr_b.clone())),
                Box::new(ast::Expression::Value(ast::Value::Integral(2))),
            )),
        );
        assert_eq!(expression("a = 1 and b = 2"), Ok(("", ans)));
    }

    #[test]
    fn test_select_expression_list() {
        let path_expr_a = PathExpr::new(vec![PathSegment::AttrName("a".to_string())]);
        let path_expr_b = PathExpr::new(vec![PathSegment::AttrName("b".to_string())]);
        let path_expr_c = PathExpr::new(vec![PathSegment::AttrName("c".to_string())]);

        let ans = vec![
            ast::SelectExpression::Star,
            ast::SelectExpression::Star,
            ast::SelectExpression::Star,
        ];
        assert_eq!(select_expression_list("*, *, *"), Ok(("", ans)));

        let ans = vec![
            ast::SelectExpression::Expression(Box::new(ast::Expression::Value(ast::Value::Integral(1))), None),
            ast::SelectExpression::Expression(Box::new(ast::Expression::Value(ast::Value::Integral(2))), None),
            ast::SelectExpression::Expression(Box::new(ast::Expression::Value(ast::Value::Integral(3))), None),
        ];
        assert_eq!(select_expression_list("1, 2, 3"), Ok(("", ans)));

        let ans = vec![
            ast::SelectExpression::Expression(Box::new(ast::Expression::Column(path_expr_a.clone())), None),
            ast::SelectExpression::Expression(Box::new(ast::Expression::Column(path_expr_b.clone())), None),
            ast::SelectExpression::Expression(Box::new(ast::Expression::Column(path_expr_c.clone())), None),
        ];
        assert_eq!(select_expression_list("a, b, c"), Ok(("", ans)));
    }

    #[test]
    fn test_select_value_statement() {
        let path_expr_a = PathExpr::new(vec![PathSegment::AttrName("a".to_string())]);

        let select_exprs = vec![ast::SelectExpression::Expression(
            Box::new(ast::Expression::Column(path_expr_a.clone())),
            None,
        )];

        let where_expr = ast::WhereExpression::new(ast::Expression::BinaryOperator(
            ast::BinaryOperator::Equal,
            Box::new(ast::Expression::Column(path_expr_a.clone())),
            Box::new(ast::Expression::Value(ast::Value::Integral(1))),
        ));
        let path_expr = PathExpr::new(vec![PathSegment::AttrName("it".to_string())]);
        let table_reference = ast::TableReference::new(path_expr, None, None);
        let ans = ast::SelectStatement::new(
            true,
            select_exprs,
            vec![table_reference],
            Some(where_expr),
            None,
            None,
            None,
            None,
        );

        assert_eq!(select_query("select value a from it where a = 1"), Ok(("", ans)));
    }

    #[test]
    fn test_select_statement() {
        let path_expr_a = PathExpr::new(vec![PathSegment::AttrName("a".to_string())]);
        let path_expr_b = PathExpr::new(vec![PathSegment::AttrName("b".to_string())]);
        let path_expr_c = PathExpr::new(vec![PathSegment::AttrName("c".to_string())]);

        let select_exprs = vec![
            ast::SelectExpression::Expression(Box::new(ast::Expression::Column(path_expr_a.clone())), None),
            ast::SelectExpression::Expression(Box::new(ast::Expression::Column(path_expr_b.clone())), None),
            ast::SelectExpression::Expression(Box::new(ast::Expression::Column(path_expr_c.clone())), None),
        ];

        let where_expr = ast::WhereExpression::new(ast::Expression::BinaryOperator(
            ast::BinaryOperator::Equal,
            Box::new(ast::Expression::Column(path_expr_a.clone())),
            Box::new(ast::Expression::Value(ast::Value::Integral(1))),
        ));
        let path_expr = PathExpr::new(vec![PathSegment::AttrName("it".to_string())]);
        let table_reference = ast::TableReference::new(path_expr, None, None);
        let ans = ast::SelectStatement::new(
            false,
            select_exprs,
            vec![table_reference],
            Some(where_expr),
            None,
            None,
            None,
            None,
        );

        assert_eq!(select_query("select a, b, c from it where a = 1"), Ok(("", ans)));
        let select_exprs = vec![
            ast::SelectExpression::Expression(Box::new(ast::Expression::Column(path_expr_a.clone())), None),
            ast::SelectExpression::Expression(Box::new(ast::Expression::Column(path_expr_b.clone())), None),
            ast::SelectExpression::Expression(Box::new(ast::Expression::Column(path_expr_c.clone())), None),
        ];

        let where_expr = ast::WhereExpression::new(ast::Expression::BinaryOperator(
            ast::BinaryOperator::Equal,
            Box::new(ast::Expression::Column(path_expr_a.clone())),
            Box::new(ast::Expression::Value(ast::Value::Integral(1))),
        ));

        let group_by_ref_a = ast::GroupByReference::new(vec!["a".to_string()], None);
        let group_by_ref_b = ast::GroupByReference::new(vec!["b".to_string()], None);
        let group_by_expr = ast::GroupByExpression::new(vec![group_by_ref_a, group_by_ref_b], None);
        let having_expr = ast::WhereExpression::new(ast::Expression::BinaryOperator(
            ast::BinaryOperator::MoreThan,
            Box::new(ast::Expression::Column(path_expr_c.clone())),
            Box::new(ast::Expression::Value(ast::Value::Integral(1))),
        ));

        let path_expr = PathExpr::new(vec![PathSegment::AttrName("it".to_string())]);
        let table_reference = ast::TableReference::new(path_expr, None, None);
        let ans = ast::SelectStatement::new(
            false,
            select_exprs,
            vec![table_reference],
            Some(where_expr),
            Some(group_by_expr),
            Some(having_expr),
            None,
            None,
        );

        assert_eq!(
            select_query("select a, b, c from it where a = 1 group by a, b having c > 1"),
            Ok(("", ans))
        );
    }

    #[test]
    fn test_func_call() {
        let path_expr_a = PathExpr::new(vec![PathSegment::AttrName("a".to_string())]);
        let path_expr_c = PathExpr::new(vec![PathSegment::AttrName("c".to_string())]);

        let ans = ast::Expression::FuncCall(
            "foo".to_string(),
            vec![
                ast::SelectExpression::Expression(Box::new(ast::Expression::Column(path_expr_a.clone())), None),
                ast::SelectExpression::Expression(Box::new(ast::Expression::Value(ast::Value::Integral(1))), None),
                ast::SelectExpression::Expression(Box::new(ast::Expression::Column(path_expr_c.clone())), None),
            ],
            None,
        );

        assert_eq!(func_call("foo(a, 1, c)"), Ok(("", ans)));

        let ans = ast::Expression::FuncCall(
            "foo".to_string(),
            vec![
                ast::SelectExpression::Expression(Box::new(ast::Expression::Column(path_expr_a.clone())), None),
                ast::SelectExpression::Expression(
                    Box::new(ast::Expression::FuncCall(
                        "bar".to_string(),
                        vec![ast::SelectExpression::Expression(
                            Box::new(ast::Expression::Value(ast::Value::Integral(1))),
                            None,
                        )],
                        None,
                    )),
                    None,
                ),
                ast::SelectExpression::Expression(Box::new(ast::Expression::Column(path_expr_c.clone())), None),
            ],
            None,
        );

        assert_eq!(func_call("foo(a, bar(1), c)"), Ok(("", ans)));

        let ans = ast::Expression::FuncCall(
            "percentile_disc".to_string(),
            vec![ast::SelectExpression::Expression(
                Box::new(ast::Expression::Value(ast::Value::Float(OrderedFloat::from(1.0)))),
                None,
            )],
            None,
        );

        assert_eq!(func_call("percentile_disc(1.0)"), Ok(("", ans)));
    }

    #[test]
    fn test_select_statement_with_func_call() {
        let path_expr_a = PathExpr::new(vec![PathSegment::AttrName("a".to_string())]);
        let path_expr_b = PathExpr::new(vec![PathSegment::AttrName("b".to_string())]);
        let path_expr_c = PathExpr::new(vec![PathSegment::AttrName("c".to_string())]);

        let select_exprs = vec![
            ast::SelectExpression::Expression(Box::new(ast::Expression::Column(path_expr_a.clone())), None),
            ast::SelectExpression::Expression(
                Box::new(ast::Expression::FuncCall(
                    "avg".to_string(),
                    vec![ast::SelectExpression::Expression(
                        Box::new(ast::Expression::Column(path_expr_b.clone())),
                        None,
                    )],
                    None,
                )),
                None,
            ),
            ast::SelectExpression::Expression(Box::new(ast::Expression::Column(path_expr_c.clone())), None),
        ];

        let where_expr = ast::WhereExpression::new(ast::Expression::BinaryOperator(
            ast::BinaryOperator::Equal,
            Box::new(ast::Expression::Column(path_expr_a.clone())),
            Box::new(ast::Expression::Value(ast::Value::Integral(1))),
        ));
        let path_expr = PathExpr::new(vec![PathSegment::AttrName("it".to_string())]);
        let table_reference = ast::TableReference::new(path_expr, None, None);
        let ans = ast::SelectStatement::new(
            false,
            select_exprs,
            vec![table_reference],
            Some(where_expr),
            None,
            None,
            None,
            None,
        );

        assert_eq!(
            select_query("select a, avg(b)   , c from it where a = 1"),
            Ok(("", ans))
        );
    }

    #[test]
    fn test_select_statement_with_limit() {
        let path_expr_a = PathExpr::new(vec![PathSegment::AttrName("a".to_string())]);
        let path_expr_b = PathExpr::new(vec![PathSegment::AttrName("b".to_string())]);
        let path_expr_c = PathExpr::new(vec![PathSegment::AttrName("c".to_string())]);

        let select_exprs = vec![
            ast::SelectExpression::Expression(Box::new(ast::Expression::Column(path_expr_a.clone())), None),
            ast::SelectExpression::Expression(Box::new(ast::Expression::Column(path_expr_b.clone())), None),
            ast::SelectExpression::Expression(Box::new(ast::Expression::Column(path_expr_c.clone())), None),
        ];

        let limit_expr = ast::LimitExpression::new(1);
        let path_expr = PathExpr::new(vec![PathSegment::AttrName("it".to_string())]);
        let table_reference = ast::TableReference::new(path_expr, None, None);
        let ans = ast::SelectStatement::new(
            false,
            select_exprs,
            vec![table_reference],
            None,
            None,
            None,
            None,
            Some(limit_expr),
        );

        assert_eq!(select_query("select a, b, c from it limit 1"), Ok(("", ans)));
    }

    #[test]
    fn test_select_statement_with_order() {
        let path_expr_a = PathExpr::new(vec![PathSegment::AttrName("a".to_string())]);
        let path_expr_b = PathExpr::new(vec![PathSegment::AttrName("b".to_string())]);
        let path_expr_c = PathExpr::new(vec![PathSegment::AttrName("c".to_string())]);

        let select_exprs = vec![
            ast::SelectExpression::Expression(Box::new(ast::Expression::Column(path_expr_a.clone())), None),
            ast::SelectExpression::Expression(Box::new(ast::Expression::Column(path_expr_b.clone())), None),
            ast::SelectExpression::Expression(Box::new(ast::Expression::Column(path_expr_c.clone())), None),
        ];

        let order_by_clause = ast::OrderByExpression::new(vec![ast::OrderingTerm::new("a", "asc")]);
        let path_expr = PathExpr::new(vec![PathSegment::AttrName("it".to_string())]);
        let table_reference = ast::TableReference::new(path_expr, None, None);
        let ans = ast::SelectStatement::new(
            false,
            select_exprs,
            vec![table_reference],
            None,
            None,
            None,
            Some(order_by_clause),
            None,
        );

        assert_eq!(select_query("select a, b, c from it order by a asc"), Ok(("", ans)));
    }

    #[test]
    fn test_select_statement_with_within_group() {
        let path_expr_a = PathExpr::new(vec![PathSegment::AttrName("a".to_string())]);
        let path_expr_c = PathExpr::new(vec![PathSegment::AttrName("c".to_string())]);

        let select_exprs = vec![
            ast::SelectExpression::Expression(Box::new(ast::Expression::Column(path_expr_a.clone())), None),
            ast::SelectExpression::Expression(Box::new(ast::Expression::Column(path_expr_c.clone())), None),
            ast::SelectExpression::Expression(
                Box::new(ast::Expression::FuncCall(
                    "percentile_disc".to_string(),
                    vec![ast::SelectExpression::Expression(
                        Box::new(ast::Expression::Value(ast::Value::Float(OrderedFloat::from(0.9)))),
                        None,
                    )],
                    Some(ast::WithinGroupClause::new(ast::OrderByExpression::new(vec![
                        ast::OrderingTerm::new("b", "asc"),
                    ]))),
                )),
                None,
            ),
        ];

        let group_by_ref_a = ast::GroupByReference::new(vec!["a".to_string()], None);
        let group_by_ref_c = ast::GroupByReference::new(vec!["c".to_string()], None);
        let group_by_expr = ast::GroupByExpression::new(vec![group_by_ref_a, group_by_ref_c], None);

        let path_expr = PathExpr::new(vec![PathSegment::AttrName("it".to_string())]);
        let table_reference = ast::TableReference::new(path_expr, None, None);
        let ans = ast::SelectStatement::new(
            false,
            select_exprs,
            vec![table_reference],
            None,
            Some(group_by_expr),
            None,
            None,
            None,
        );

        assert_eq!(
            select_query("select a, c, percentile_disc(0.9) within group (order by b asc) from it group by a, c "),
            Ok(("", ans))
        );
    }

    #[test]
    fn test_select_statement_with_as() {
        let path_expr_a = PathExpr::new(vec![PathSegment::AttrName("a".to_string())]);
        let path_expr_b = PathExpr::new(vec![PathSegment::AttrName("b".to_string())]);

        let select_exprs = vec![
            ast::SelectExpression::Expression(
                Box::new(ast::Expression::Column(path_expr_a.clone())),
                Some("aa".to_string()),
            ),
            ast::SelectExpression::Expression(
                Box::new(ast::Expression::FuncCall(
                    "foo".to_string(),
                    vec![ast::SelectExpression::Expression(
                        Box::new(ast::Expression::Column(path_expr_b.clone())),
                        None,
                    )],
                    None,
                )),
                Some("bb".to_string()),
            ),
            ast::SelectExpression::Expression(
                Box::new(ast::Expression::BinaryOperator(
                    ast::BinaryOperator::Plus,
                    Box::new(ast::Expression::Value(ast::Value::Integral(1))),
                    Box::new(ast::Expression::Value(ast::Value::Integral(1))),
                )),
                Some("cc".to_string()),
            ),
        ];

        let path_expr = PathExpr::new(vec![PathSegment::AttrName("it".to_string())]);
        let table_reference = ast::TableReference::new(path_expr, None, None);
        let ans = ast::SelectStatement::new(false, select_exprs, vec![table_reference], None, None, None, None, None);
        assert_eq!(
            select_query("select a as aa, foo( b ) as bb, 1+1 as cc from it"),
            Ok(("", ans))
        );
    }

    #[test]
    fn test_select_stmt_error() {
        assert_eq!(
            select_query("select select from it"),
            Err(nom::Err::Failure(VerboseError {
                errors: vec![
                    (
                        " from it",
                        nom::error::VerboseErrorKind::Nom(nom::error::ErrorKind::Alpha)
                    ),
                    (
                        " select from it",
                        nom::error::VerboseErrorKind::Context("select_expression_list")
                    )
                ]
            }))
        );

        assert_eq!(
            select_query("select * from it where limit 1"),
            Err(nom::Err::Failure(VerboseError {
                errors: vec![(" 1", nom::error::VerboseErrorKind::Nom(nom::error::ErrorKind::Alpha))]
            }))
        );
    }

    #[test]
    fn test_parse_expression_atom() {
        let (_, ans) = parse_expression_atom("1").unwrap();
        let expected = ast::Expression::Value(ast::Value::Integral(1));

        assert_eq!(expected, ans);
    }

    #[test]
    fn test_parse_expression_op() {
        let (_, ans) = parse_expression_op("+").unwrap();
        let expected = "+";
        assert_eq!(expected, ans);

        let (_, ans) = parse_expression_op("*").unwrap();
        let expected = "*";
        assert_eq!(expected, ans);
    }

    #[test]
    fn test_parse_expression_at_precedence() {
        let mut precedence_table: HashMap<String, (u32, bool)> = HashMap::new();
        precedence_table.insert("*".to_string(), (7, true));
        precedence_table.insert("/".to_string(), (7, true));
        precedence_table.insert("+".to_string(), (6, true));
        precedence_table.insert("-".to_string(), (6, true));
        precedence_table.insert("<".to_string(), (4, true));
        precedence_table.insert("<=".to_string(), (4, true));
        precedence_table.insert(">".to_string(), (4, true));
        precedence_table.insert(">=".to_string(), (4, true));
        precedence_table.insert("=".to_string(), (3, true));
        precedence_table.insert("!=".to_string(), (3, true));
        precedence_table.insert("and".to_string(), (2, true));
        precedence_table.insert("or".to_string(), (1, true));
        let (_, ans) = parse_expression_at_precedence(" 1 +1*2- 3", 1, &precedence_table).unwrap();
        let expected = ast::Expression::BinaryOperator(
            ast::BinaryOperator::Minus,
            Box::new(ast::Expression::BinaryOperator(
                ast::BinaryOperator::Plus,
                Box::new(ast::Expression::Value(ast::Value::Integral(1))),
                Box::new(ast::Expression::BinaryOperator(
                    ast::BinaryOperator::Times,
                    Box::new(ast::Expression::Value(ast::Value::Integral(1))),
                    Box::new(ast::Expression::Value(ast::Value::Integral(2))),
                )),
            )),
            Box::new(ast::Expression::Value(ast::Value::Integral(3))),
        );

        assert_eq!(expected, ans);
    }

    #[test]
    fn test_path_expr() {
        let (_, ans) = path_expr("a.b.c[0].d").unwrap();
        let path_segments = vec![
            ast::PathSegment::AttrName("a".to_string()),
            ast::PathSegment::AttrName("b".to_string()),
            ast::PathSegment::ArrayIndex("c".to_string(), 0),
            ast::PathSegment::AttrName("d".to_string()),
        ];
        let expected = ast::PathExpr::new(path_segments);
        assert_eq!(expected, ans);
    }

    #[test]
    fn test_single_quote_literal_string() {
        let (_, ans) = single_quote_string_literal("'a'").unwrap();
        let expected = "a".to_string();
        assert_eq!(expected, ans);
    }

    #[test]
    fn test_tuple_constructor() {
        let (_, ans) = tuple_constructor("{'a':a, 'b':b}").unwrap();
        let key_values = vec![
            (
                "a".to_string(),
                ast::Expression::Column(ast::PathExpr::new(vec![ast::PathSegment::AttrName("a".to_string())])),
            ),
            (
                "b".to_string(),
                ast::Expression::Column(ast::PathExpr::new(vec![ast::PathSegment::AttrName("b".to_string())])),
            ),
        ];

        let expected = ast::TupleConstructor { key_values };
        assert_eq!(expected, ans);
    }

    #[test]
    fn test_array_constructor() {
        let (_, ans) = array_constructor("[a, b]").unwrap();
        let values = vec![
            ast::Expression::Column(ast::PathExpr::new(vec![ast::PathSegment::AttrName("a".to_string())])),
            ast::Expression::Column(ast::PathExpr::new(vec![ast::PathSegment::AttrName("b".to_string())])),
        ];

        let expected = ast::ArrayConstructor { values };
        assert_eq!(expected, ans);
    }
}
