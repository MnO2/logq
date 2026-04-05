use super::ast;
use ordered_float::OrderedFloat;
use std::str::FromStr;

use nom::{
    branch::alt,
    bytes::complete::{escaped, tag, tag_no_case},
    character::complete::{char, digit1, multispace0, multispace1, none_of, one_of, space0, space1},
    combinator::{cut, map, map_res, not, opt},
    error::context,
    multi::{many0, many1, separated_list0},
    number::complete,
    sequence::{delimited, pair, preceded, terminated, tuple},
    AsChar, IResult, InputTakeAtPosition,
};

use crate::syntax::ast::{
    ArrayConstructor, FromClause, JoinType, PathExpr, PathSegment, SelectClause, SelectExpression, TableReference,
    TupleConstructor,
};
use hashbrown::hash_map::HashMap;

fn case_when_branch(i: &str) -> IResult<&str, (ast::Expression, ast::Expression), nom::error::Error<&str>> {
    let (remaining_input, (_, condition, _, then_expr)) = tuple((
        tuple((multispace0, tag_no_case("when"), space1)),
        expression,
        tuple((multispace0, tag_no_case("then"), space1)),
        factor,
    ))(i)?;

    Ok((remaining_input, (condition, then_expr)))
}

fn case_when_expression(i: &str) -> IResult<&str, ast::CaseWhenExpression, nom::error::Error<&str>> {
    let (remaining_input, (_, branches, else_expr, _)) = tuple((
        tag_no_case("case"),
        many1(case_when_branch),
        opt(delimited(
            tuple((multispace0, tag_no_case("else"), space1)),
            factor,
            multispace0,
        )),
        tag_no_case("end"),
    ))(i)?;

    Ok((
        remaining_input,
        ast::CaseWhenExpression {
            branches,
            else_expr: else_expr.map(|n| Box::new(n)),
        },
    ))
}

fn double_quote_string_literal_interior(i: &str) -> IResult<&str, &str, nom::error::Error<&str>> {
    escaped(none_of("\""), '\\', one_of("\"n\\"))(i)
}

fn double_quote_string_literal(i: &str) -> IResult<&str, ast::Value, nom::error::Error<&str>> {
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

fn single_quote_string_literal_interior(i: &str) -> IResult<&str, &str, nom::error::Error<&str>> {
    escaped(none_of("'"), '\\', one_of("\"n\\"))(i)
}

fn single_quote_string_literal(i: &str) -> IResult<&str, String, nom::error::Error<&str>> {
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

/// Check if a string is a SQL keyword using first-char dispatch.
/// Partitions ~40 keywords by their first letter so average comparison
/// count drops from 40 to ~2-3 per lookup.
fn is_keyword(s: &str) -> bool {
    let first = s.as_bytes()[0] | 0x20; // ASCII lowercase
    match first {
        b'a' => ["all", "and", "as", "at", "asc"].iter().any(|kw| kw.eq_ignore_ascii_case(s)),
        b'b' => ["between", "by"].iter().any(|kw| kw.eq_ignore_ascii_case(s)),
        b'c' => ["case", "cross", "cast"].iter().any(|kw| kw.eq_ignore_ascii_case(s)),
        b'd' => ["desc", "distinct"].iter().any(|kw| kw.eq_ignore_ascii_case(s)),
        b'e' => ["else", "end", "except", "exists"].iter().any(|kw| kw.eq_ignore_ascii_case(s)),
        b'f' => ["false", "from"].iter().any(|kw| kw.eq_ignore_ascii_case(s)),
        b'g' => ["group"].iter().any(|kw| kw.eq_ignore_ascii_case(s)),
        b'h' => ["having"].iter().any(|kw| kw.eq_ignore_ascii_case(s)),
        b'i' => ["in", "inner", "intersect", "is"].iter().any(|kw| kw.eq_ignore_ascii_case(s)),
        b'j' => ["join"].iter().any(|kw| kw.eq_ignore_ascii_case(s)),
        b'l' => ["lateral", "left", "like", "limit"].iter().any(|kw| kw.eq_ignore_ascii_case(s)),
        b'm' => ["missing"].iter().any(|kw| kw.eq_ignore_ascii_case(s)),
        b'n' => ["not", "null"].iter().any(|kw| kw.eq_ignore_ascii_case(s)),
        b'o' => ["on", "or", "order"].iter().any(|kw| kw.eq_ignore_ascii_case(s)),
        b's' => ["select"].iter().any(|kw| kw.eq_ignore_ascii_case(s)),
        b't' => ["then", "true"].iter().any(|kw| kw.eq_ignore_ascii_case(s)),
        b'u' => ["union"].iter().any(|kw| kw.eq_ignore_ascii_case(s)),
        b'v' => ["value"].iter().any(|kw| kw.eq_ignore_ascii_case(s)),
        b'w' => ["when", "where", "within"].iter().any(|kw| kw.eq_ignore_ascii_case(s)),
        _ => false,
    }
}

fn identifier<'a, E: nom::error::ParseError<&'a str>>(input: &'a str) -> IResult<&'a str, &'a str, E>
where
{
    fn is_alphanumeric_or_underscore(chr: char) -> bool {
        chr.is_alphanumeric() || chr == '_'
    }

    fn start_with_number(s: &str) -> bool {
        s.as_bytes().first().map_or(false, |b| b.is_ascii_digit())
    }

    fn all_underscore(s: &str) -> bool {
        s.bytes().all(|b| b == b'_')
    }

    let result = input.split_at_position1_complete(
        |item| !is_alphanumeric_or_underscore(item.as_char()),
        nom::error::ErrorKind::Alpha,
    );

    match result {
        Ok((i, o)) => {
            if start_with_number(o) || all_underscore(o) || is_keyword(o) {
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

fn boolean(i: &str) -> IResult<&str, ast::Value, nom::error::Error<&str>> {
    alt((
        map(tag_no_case("true"), |_| ast::Value::Boolean(true)),
        map(tag_no_case("false"), |_| ast::Value::Boolean(false)),
    ))(i)
}

fn float(i: &str) -> IResult<&str, ast::Value, nom::error::Error<&str>> {
    map(complete::float, |f| ast::Value::Float(OrderedFloat::from(f)))(i)
}

fn integral(i: &str) -> IResult<&str, ast::Value, nom::error::Error<&str>> {
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

fn null_literal(i: &str) -> IResult<&str, ast::Value, nom::error::Error<&str>> {
    let (i, _) = tag_no_case("null")(i)?;
    // Ensure "null" is not followed by alphanumeric/underscore (e.g., "nullif")
    if i.starts_with(|c: char| c.is_alphanumeric() || c == '_') {
        return Err(nom::Err::Error(nom::error::Error::new(i, nom::error::ErrorKind::Tag)));
    }
    Ok((i, ast::Value::Null))
}

fn missing_literal(i: &str) -> IResult<&str, ast::Value, nom::error::Error<&str>> {
    let (i, _) = tag_no_case("missing")(i)?;
    // Ensure "missing" is not followed by alphanumeric/underscore
    if i.starts_with(|c: char| c.is_alphanumeric() || c == '_') {
        return Err(nom::Err::Error(nom::error::Error::new(i, nom::error::ErrorKind::Tag)));
    }
    Ok((i, ast::Value::Missing))
}

fn value(i: &str) -> IResult<&str, ast::Value, nom::error::Error<&str>> {
    alt((integral, float, boolean, double_quote_string_literal, null_literal, missing_literal))(i)
}

fn parens(i: &str) -> IResult<&str, ast::Expression, nom::error::Error<&str>> {
    delimited(space0, delimited(tag("("), expression, tag(")")), space0)(i)
}

fn order_by_clause_for_within_group(i: &str) -> IResult<&str, ast::OrderByExpression, nom::error::Error<&str>> {
    map(
        preceded(tuple((tag_no_case("order"), space1, tag_no_case("by"), space1)), ordering_term),
        |item| ast::OrderByExpression::new(vec![item]),
    )(i)
}

fn within_group_clause(i: &str) -> IResult<&str, ast::WithinGroupClause, nom::error::Error<&str>> {
    map(
        preceded(
            tuple((space1, tag_no_case("within"), space1, tag_no_case("group"), space1)),
            delimited(tag("("), order_by_clause_for_within_group, tag(")")),
        ),
        ast::WithinGroupClause::new,
    )(i)
}

fn func_call(i: &str) -> IResult<&str, ast::Expression, nom::error::Error<&str>> {
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

fn select_column_reference(i: &str) -> IResult<&str, ast::PathExpr, nom::error::Error<&str>> {
    terminated(path_expr, not(char('(')))(i)
}

fn subquery(i: &str) -> IResult<&str, ast::Expression, nom::error::Error<&str>> {
    map(
        delimited(
            tuple((space0, tag("("), space0)),
            select_query,
            tuple((space0, tag(")"), space0)),
        ),
        |stmt| ast::Expression::Subquery(Box::new(stmt)),
    )(i)
}

fn factor(i: &str) -> IResult<&str, ast::Expression, nom::error::Error<&str>> {
    delimited(
        space0,
        alt((
            subquery,   // Try subquery first (parenthesized SELECT)
            parens,
            map(value, ast::Expression::Value),
            func_call,
            map(select_column_reference, |path_expr| ast::Expression::Column(path_expr)),
        )),
        space0,
    )(i)
}

fn expression_term_opt_not(i: &str) -> IResult<&str, ast::Expression, nom::error::Error<&str>> {
    map(
        pair(opt(tuple((space1, tag_no_case("not"), space1))), factor),
        |(opt_not, factor)| {
            if opt_not.is_some() {
                ast::Expression::UnaryOperator(ast::UnaryOperator::Not, Box::new(factor))
            } else {
                factor
            }
        },
    )(i)
}

fn expression(i: &str) -> IResult<&str, ast::Expression, nom::error::Error<&str>> {
    lazy_static! {
        static ref PRECEDENCE_TABLE: HashMap<&'static str, (u32, bool)> = {
            let mut m = HashMap::new();
            m.insert("*", (7, true));
            m.insert("/", (7, true));
            m.insert("+", (6, true));
            m.insert("-", (6, true));
            m.insert("||", (6, true));
            m.insert("<", (4, true));
            m.insert("<=", (4, true));
            m.insert(">", (4, true));
            m.insert(">=", (4, true));
            m.insert("=", (3, true));
            m.insert("!=", (3, true));
            m.insert("and", (2, true));
            m.insert("or", (1, true));
            m
        };
    }

    let (i1, expr) = parse_expression_at_precedence(i, 1, &PRECEDENCE_TABLE)?;
    Ok((i1, expr))
}

fn select_expression(i: &str) -> IResult<&str, ast::SelectExpression, nom::error::Error<&str>> {
    preceded(
        space0,
        alt((
            map(char('*'), |_| ast::SelectExpression::Star),
            map(
                pair(
                    expression,
                    opt(preceded(tuple((space0, tag_no_case("as"), space1)), identifier)),
                ),
                |(e, name_opt)| ast::SelectExpression::Expression(Box::new(e), name_opt.map(|s| s.to_string())),
            ),
        )),
    )(i)
}

fn select_expression_list(i: &str) -> IResult<&str, Vec<SelectExpression>, nom::error::Error<&str>> {
    context(
        "select_expression_list",
        terminated(separated_list0(preceded(space0, char(',')), select_expression), space0),
    )(i)
}

fn where_expression(i: &str) -> IResult<&str, ast::WhereExpression, nom::error::Error<&str>> {
    map(preceded(tag_no_case("where"), expression), ast::WhereExpression::new)(i)
}

fn column_name_in_group_by(i: &str) -> IResult<&str, PathExpr, nom::error::Error<&str>> {
    terminated(path_expr, not(char('(')))(i)
}

fn column_factor(i: &str) -> IResult<&str, ast::Expression, nom::error::Error<&str>> {
    delimited(
        space0,
        alt((
            parens,
            map(value, ast::Expression::Value),
            func_call,
            map(column_name_in_group_by, |path_expr| ast::Expression::Column(path_expr)),
        )),
        space0,
    )(i)
}

fn column_reference(i: &str) -> IResult<&str, ast::GroupByReference, nom::error::Error<&str>> {
    map(
        tuple((
            column_factor,
            opt(preceded(tuple((space0, tag_no_case("as"), space1)), identifier)),
        )),
        |(column_expr, as_clasue)| ast::GroupByReference::new(column_expr, as_clasue.map(|s| s.to_string())),
    )(i)
}

fn column_expression_list(i: &str) -> IResult<&str, Vec<ast::GroupByReference>, nom::error::Error<&str>> {
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

fn path_bracket(i: &str) -> IResult<&str, ast::Value, nom::error::Error<&str>> {
    delimited(tag("["), integral, tag("]"))(i)
}

/// Parse a bracket suffix: either `[*]` (wildcard) or `[<integer>]` (array index)
fn path_bracket_or_wildcard(i: &str) -> IResult<&str, PathSegment, nom::error::Error<&str>> {
    alt((
        map(tag("[*]"), |_| PathSegment::Wildcard),
        map(path_bracket, |v| match v {
            ast::Value::Integral(idx) => PathSegment::ArrayIndex(String::new(), idx as usize),
            _ => unreachable!(),
        }),
    ))(i)
}

/// A trailing segment after the first identifier:
/// - `.<identifier>` (possibly followed by `[<index>]` or `[*]`)
/// - `.*` (wildcard attr)
/// - `[<index>]` (array index on previous segment — handled separately)
/// - `[*]` (array wildcard on previous segment — handled separately)
enum TrailingSegment {
    DotAttr(String, Option<PathSegment>), // .attr possibly followed by bracket
    DotWildcardAttr,                       // .*
    Bracket(PathSegment),                  // [index] or [*]
}

fn trailing_dot_wildcard_attr(i: &str) -> IResult<&str, TrailingSegment, nom::error::Error<&str>> {
    map(tag(".*"), |_| TrailingSegment::DotWildcardAttr)(i)
}

fn trailing_dot_attr(i: &str) -> IResult<&str, TrailingSegment, nom::error::Error<&str>> {
    map(
        pair(preceded(char('.'), identifier), opt(path_bracket_or_wildcard)),
        |(name, bracket)| TrailingSegment::DotAttr(name.to_string(), bracket),
    )(i)
}

fn trailing_bracket(i: &str) -> IResult<&str, TrailingSegment, nom::error::Error<&str>> {
    map(path_bracket_or_wildcard, TrailingSegment::Bracket)(i)
}

fn path_expr(i: &str) -> IResult<&str, PathExpr, nom::error::Error<&str>> {
    let (rest, (first_id, first_bracket, trailing)) = terminated(
        tuple((
            identifier,
            opt(path_bracket_or_wildcard),
            many0(alt((trailing_dot_wildcard_attr, trailing_dot_attr, trailing_bracket))),
        )),
        not(char('(')),
    )(i)?;

    let mut segments = Vec::new();

    // First segment
    match first_bracket {
        Some(PathSegment::ArrayIndex(_, idx)) => {
            segments.push(PathSegment::ArrayIndex(first_id.to_string(), idx));
        }
        Some(PathSegment::Wildcard) => {
            segments.push(PathSegment::AttrName(first_id.to_string()));
            segments.push(PathSegment::Wildcard);
        }
        Some(_) => unreachable!(),
        None => {
            segments.push(PathSegment::AttrName(first_id.to_string()));
        }
    }

    // Trailing segments
    for seg in trailing {
        match seg {
            TrailingSegment::DotWildcardAttr => {
                segments.push(PathSegment::WildcardAttr);
            }
            TrailingSegment::DotAttr(name, bracket) => match bracket {
                Some(PathSegment::ArrayIndex(_, idx)) => {
                    segments.push(PathSegment::ArrayIndex(name, idx));
                }
                Some(PathSegment::Wildcard) => {
                    segments.push(PathSegment::AttrName(name));
                    segments.push(PathSegment::Wildcard);
                }
                Some(_) => unreachable!(),
                None => {
                    segments.push(PathSegment::AttrName(name));
                }
            },
            TrailingSegment::Bracket(b) => match b {
                PathSegment::ArrayIndex(_, idx) => {
                    // Attach to previous AttrName if possible
                    if let Some(last) = segments.last_mut() {
                        if let PathSegment::AttrName(name) = last {
                            let name = name.clone();
                            *last = PathSegment::ArrayIndex(name, idx);
                        } else {
                            segments.push(PathSegment::ArrayIndex(String::new(), idx));
                        }
                    }
                }
                PathSegment::Wildcard => {
                    segments.push(PathSegment::Wildcard);
                }
                _ => unreachable!(),
            },
        }
    }

    Ok((rest, PathExpr::new(segments)))
}

fn table_reference(i: &str) -> IResult<&str, ast::TableReference, nom::error::Error<&str>> {
    map(
        tuple((
            path_expr,
            opt(preceded(tuple((space0, tag_no_case("as"), space1)), identifier)),
            opt(preceded(tuple((space0, tag_no_case("at"), space1)), identifier)),
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

fn table_reference_separator(i: &str) -> IResult<&str, (), nom::error::Error<&str>> {
    alt((
        map(preceded(space0, char(',')), |_| ()),
        map(
            tuple((
                multispace1,
                tag_no_case("cross"),
                multispace1,
                tag_no_case("join"),
            )),
            |_| (),
        ),
    ))(i)
}

/// Parse a comma-separated or CROSS JOIN separated list of table references into FromClause::Tables.
fn table_reference_list(i: &str) -> IResult<&str, FromClause, nom::error::Error<&str>> {
    context(
        "table_reference_list",
        map(
            terminated(
                separated_list0(table_reference_separator, preceded(space0, table_reference)),
                space0,
            ),
            FromClause::Tables,
        ),
    )(i)
}

/// Parse LEFT [OUTER] JOIN ... ON ... returning (JoinType, TableReference, Expression)
fn left_join_clause(i: &str) -> IResult<&str, (JoinType, ast::TableReference, ast::Expression), nom::error::Error<&str>> {
    let (i, _) = multispace0(i)?;
    let (i, _) = tag_no_case("left")(i)?;
    let (i, _) = multispace1(i)?;
    // Optional "OUTER" keyword
    let (i, _) = opt(terminated(tag_no_case("outer"), multispace1))(i)?;
    let (i, _) = tag_no_case("join")(i)?;
    let (i, _) = multispace1(i)?;
    let (i, right_ref) = table_reference(i)?;
    let (i, _) = multispace1(i)?;
    let (i, _) = tag_no_case("on")(i)?;
    let (i, _) = multispace1(i)?;
    let (i, on_condition) = expression(i)?;

    Ok((i, (JoinType::Left, right_ref, on_condition)))
}

fn having_expression(i: &str) -> IResult<&str, ast::WhereExpression, nom::error::Error<&str>> {
    map(preceded(tag_no_case("having"), expression), ast::WhereExpression::new)(i)
}

fn group_by_expression(i: &str) -> IResult<&str, ast::GroupByExpression, nom::error::Error<&str>> {
    map(
        preceded(
            tuple((tag_no_case("group"), space1, tag_no_case("by"), space1)),
            pair(
                column_expression_list,
                opt(preceded(
                    tuple((space0, tag_no_case("group"), space1, tag_no_case("as"), space1)),
                    identifier,
                )),
            ),
        ),
        |(group_by_references, group_as_clause)| {
            ast::GroupByExpression::new(group_by_references, group_as_clause.map(|s| s.to_string()))
        },
    )(i)
}

fn limit_expression(i: &str) -> IResult<&str, ast::LimitExpression, nom::error::Error<&str>> {
    map(preceded(tuple((tag_no_case("limit"), space1)), digit1), |s: &str| {
        let v = s.parse::<u32>().unwrap();
        ast::LimitExpression::new(v)
    })(i)
}

fn ordering_term(i: &str) -> IResult<&str, ast::OrderingTerm, nom::error::Error<&str>> {
    map(
        pair(
            column_name_in_group_by,
            preceded(space1, alt((tag_no_case("asc"), tag_no_case("desc")))),
        ),
        |(column_name, ordering)| ast::OrderingTerm::new(column_name, ordering),
    )(i)
}

fn order_by_clause(i: &str) -> IResult<&str, ast::OrderByExpression, nom::error::Error<&str>> {
    map(
        preceded(
            tuple((tag_no_case("order"), space1, tag_no_case("by"), space1)),
            terminated(separated_list0(preceded(space0, char(',')), ordering_term), space0),
        ),
        ast::OrderByExpression::new,
    )(i)
}

fn from_clause(i: &str) -> IResult<&str, FromClause, nom::error::Error<&str>> {
    let (i, _) = tag_no_case("from")(i)?;
    let (i, _) = space1(i)?;
    let (i, base) = table_reference_list(i)?;

    // Try to parse zero or more LEFT JOIN clauses chained after the base
    let (i, joins) = many0(left_join_clause)(i)?;

    let result = joins.into_iter().fold(base, |acc, (join_type, right_ref, on_expr)| {
        FromClause::Join {
            left: Box::new(acc),
            right: right_ref,
            join_type,
            condition: Some(on_expr),
        }
    });

    let (i, _) = space0(i)?;
    Ok((i, result))
}

fn cast_expression(i: &str) -> IResult<&str, ast::Expression, nom::error::Error<&str>> {
    let (i, _) = space0(i)?;
    let (i, _) = tag_no_case("cast")(i)?;
    let (i, _) = multispace0(i)?;
    let (i, _) = tag("(")(i)?;
    let (i, _) = multispace0(i)?;
    let (i, expr) = expression(i)?;
    let (i, _) = multispace0(i)?;
    let (i, _) = tag_no_case("as")(i)?;
    let (i, _) = multispace1(i)?;
    let (i, cast_type) = alt((
        map(tag_no_case("integer"), |_| ast::CastType::Int),
        map(tag_no_case("int"), |_| ast::CastType::Int),
        map(tag_no_case("float"), |_| ast::CastType::Float),
        map(tag_no_case("varchar"), |_| ast::CastType::Varchar),
        map(tag_no_case("string"), |_| ast::CastType::Varchar),
        map(tag_no_case("boolean"), |_| ast::CastType::Boolean),
        map(tag_no_case("bool"), |_| ast::CastType::Boolean),
    ))(i)?;
    let (i, _) = multispace0(i)?;
    let (i, _) = tag(")")(i)?;
    Ok((i, ast::Expression::Cast(Box::new(expr), cast_type)))
}

fn parse_expression_atom(i: &str) -> IResult<&str, ast::Expression, nom::error::Error<&str>> {
    alt((
        map(case_when_expression, |n| ast::Expression::CaseWhenExpression(n)),
        cast_expression,
        expression_term_opt_not,
    ))(i)
}

fn parse_expression_op(i: &str) -> IResult<&str, &str, nom::error::Error<&str>> {
    alt((
        tag("||"),
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
        tag_no_case("and"),
        tag_no_case("or"),
    ))(i)
}

fn parse_postfix_is<'a>(input: &'a str, expr: ast::Expression) -> IResult<&'a str, ast::Expression, nom::error::Error<&'a str>> {
    let i = multispace0::<&str, nom::error::Error<&str>>(input)?.0;
    if let Ok((i, _)) = tag_no_case::<&str, &str, nom::error::Error<&str>>("is")(i) {
        if let Ok((i, _)) = multispace1::<&str, nom::error::Error<&str>>(i) {
            if let Ok((i, _)) = tag_no_case::<&str, &str, nom::error::Error<&str>>("not")(i) {
                if let Ok((i, _)) = multispace1::<&str, nom::error::Error<&str>>(i) {
                    if let Ok((i, _)) = tag_no_case::<&str, &str, nom::error::Error<&str>>("null")(i) {
                        return Ok((i, ast::Expression::IsNotNull(Box::new(expr))));
                    }
                    if let Ok((i, _)) = tag_no_case::<&str, &str, nom::error::Error<&str>>("missing")(i) {
                        return Ok((i, ast::Expression::IsNotMissing(Box::new(expr))));
                    }
                }
            }
            if let Ok((i, _)) = tag_no_case::<&str, &str, nom::error::Error<&str>>("null")(i) {
                return Ok((i, ast::Expression::IsNull(Box::new(expr))));
            }
            if let Ok((i, _)) = tag_no_case::<&str, &str, nom::error::Error<&str>>("missing")(i) {
                return Ok((i, ast::Expression::IsMissing(Box::new(expr))));
            }
        }
    }
    Ok((input, expr))
}

fn parse_postfix_like<'a>(input: &'a str, expr: ast::Expression) -> IResult<&'a str, ast::Expression, nom::error::Error<&'a str>> {
    let i = multispace0::<&str, nom::error::Error<&str>>(input)?.0;
    // Check for NOT LIKE first (before LIKE)
    if let Ok((i, _)) = tag_no_case::<&str, &str, nom::error::Error<&str>>("not")(i) {
        if let Ok((i, _)) = multispace1::<&str, nom::error::Error<&str>>(i) {
            if let Ok((i, _)) = tag_no_case::<&str, &str, nom::error::Error<&str>>("like")(i) {
                let (i, _) = multispace1(i)?;
                let (i, pattern) = parse_expression_atom(i)?;
                return Ok((i, ast::Expression::NotLike(Box::new(expr), Box::new(pattern))));
            }
        }
    }
    if let Ok((i, _)) = tag_no_case::<&str, &str, nom::error::Error<&str>>("like")(i) {
        let (i, _) = multispace1::<&str, nom::error::Error<&str>>(i)?;
        let (i, pattern) = parse_expression_atom(i)?;
        return Ok((i, ast::Expression::Like(Box::new(expr), Box::new(pattern))));
    }
    Ok((input, expr))
}

fn parse_postfix_in<'a>(input: &'a str, expr: ast::Expression) -> IResult<&'a str, ast::Expression, nom::error::Error<&'a str>> {
    let i = multispace0::<&str, nom::error::Error<&str>>(input)?.0;
    // Check NOT IN first
    let (i, negated) = if let Ok((i2, _)) = tag_no_case::<&str, &str, nom::error::Error<&str>>("not")(i) {
        if let Ok((i3, _)) = multispace1::<&str, nom::error::Error<&str>>(i2) {
            if let Ok((i4, _)) = tag_no_case::<&str, &str, nom::error::Error<&str>>("in")(i3) {
                // Ensure "in" is not a prefix of another keyword (e.g., "intersect")
                if i4.starts_with(|c: char| c.is_alphanumeric() || c == '_') {
                    return Ok((input, expr));
                }
                (i4, true)
            } else {
                return Ok((input, expr));
            }
        } else {
            return Ok((input, expr));
        }
    } else if let Ok((i2, _)) = tag_no_case::<&str, &str, nom::error::Error<&str>>("in")(i) {
        // Ensure "in" is not a prefix of another keyword (e.g., "intersect")
        if i2.starts_with(|c: char| c.is_alphanumeric() || c == '_') {
            return Ok((input, expr));
        }
        (i2, false)
    } else {
        return Ok((input, expr));
    };

    let (i, _) = multispace0(i)?;
    let (i, _) = tag("(")(i)?;
    let (i, _) = multispace0(i)?;
    // Parse comma-separated list of expressions
    let (i, first) = expression(i)?;
    let (i, rest) = many0(|i: &'a str| {
        let (i, _) = multispace0(i)?;
        let (i, _) = tag(",")(i)?;
        let (i, _) = multispace0(i)?;
        expression(i)
    })(i)?;
    let (i, _) = multispace0(i)?;
    let (i, _) = tag(")")(i)?;

    let mut list = vec![first];
    list.extend(rest);

    if negated {
        Ok((i, ast::Expression::NotIn(Box::new(expr), list)))
    } else {
        Ok((i, ast::Expression::In(Box::new(expr), list)))
    }
}

fn parse_postfix_between<'a>(input: &'a str, expr: ast::Expression) -> IResult<&'a str, ast::Expression, nom::error::Error<&'a str>> {
    let i = multispace0::<&str, nom::error::Error<&str>>(input)?.0;
    // Check for NOT BETWEEN first
    if let Ok((i, _)) = tag_no_case::<&str, &str, nom::error::Error<&str>>("not")(i) {
        if let Ok((i, _)) = multispace1::<&str, nom::error::Error<&str>>(i) {
            if let Ok((i, _)) = tag_no_case::<&str, &str, nom::error::Error<&str>>("between")(i) {
                let (i, _) = multispace1(i)?;
                let (i, lo) = parse_expression_atom(i)?;
                let (i, _) = multispace0(i)?;
                let (i, _) = tag_no_case("and")(i)?;
                let (i, _) = multispace0(i)?;
                let (i, hi) = parse_expression_atom(i)?;
                return Ok((i, ast::Expression::NotBetween(Box::new(expr), Box::new(lo), Box::new(hi))));
            }
        }
    }
    if let Ok((i, _)) = tag_no_case::<&str, &str, nom::error::Error<&str>>("between")(i) {
        let (i, _) = multispace1::<&str, nom::error::Error<&str>>(i)?;
        let (i, lo) = parse_expression_atom(i)?;
        let (i, _) = multispace0(i)?;
        let (i, _) = tag_no_case("and")(i)?;
        let (i, _) = multispace0(i)?;
        let (i, hi) = parse_expression_atom(i)?;
        return Ok((i, ast::Expression::Between(Box::new(expr), Box::new(lo), Box::new(hi))));
    }
    Ok((input, expr))
}

fn parse_expression_at_precedence<'a>(
    i0: &'a str,
    current_precedence: u32,
    precedence_table: &HashMap<&str, (u32, bool)>,
) -> IResult<&'a str, ast::Expression, nom::error::Error<&'a str>> {
    let (i1, expr) = parse_expression_atom(i0)?;
    let (i1, expr) = parse_postfix_is(i1, expr)?;
    let (i1, expr) = parse_postfix_like(i1, expr)?;
    let (i1, expr) = parse_postfix_in(i1, expr)?;
    let (mut i1, mut expr) = parse_postfix_between(i1, expr)?;
    while let Ok((i2, op)) = parse_expression_op(i1) {
        let (op_precedence, op_left_associative) = *precedence_table.get(op.to_ascii_lowercase().as_str()).unwrap();

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

fn tuple_constructor_expression_term(i: &str) -> IResult<&str, (String, ast::Expression), nom::error::Error<&str>> {
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

fn tuple_constructor_expression_list(i: &str) -> IResult<&str, Vec<(String, ast::Expression)>, nom::error::Error<&str>> {
    context(
        "tuple_constructor_expression_list",
        terminated(
            separated_list0(preceded(space0, char(',')), tuple_constructor_expression_term),
            space0,
        ),
    )(i)
}

fn tuple_constructor(i: &str) -> IResult<&str, ast::TupleConstructor, nom::error::Error<&str>> {
    map(
        delimited(
            space0,
            delimited(tag("{"), tuple_constructor_expression_list, tag("}")),
            space0,
        ),
        |v| TupleConstructor { key_values: v },
    )(i)
}

fn array_constructor_expression_list(i: &str) -> IResult<&str, Vec<ast::Expression>, nom::error::Error<&str>> {
    context(
        "array_constructor_expression_list",
        terminated(separated_list0(preceded(space0, char(',')), expression), space0),
    )(i)
}

fn array_constructor(i: &str) -> IResult<&str, ast::ArrayConstructor, nom::error::Error<&str>> {
    map(
        delimited(
            space0,
            delimited(tag("["), array_constructor_expression_list, tag("]")),
            space0,
        ),
        |v| ArrayConstructor { values: v },
    )(i)
}

fn value_constructor(i: &str) -> IResult<&str, ast::SelectClause, nom::error::Error<&str>> {
    delimited(
        space0,
        preceded(
            pair(tag_no_case("value"), space1),
            alt((
                map(tuple_constructor, |v| {
                    ast::SelectClause::ValueConstructor(ast::ValueConstructor::TupleConstructor(v))
                }),
                map(array_constructor, |v| {
                    ast::SelectClause::ValueConstructor(ast::ValueConstructor::ArrayConstructor(v))
                }),
                map(expression, |v| {
                    ast::SelectClause::ValueConstructor(ast::ValueConstructor::Expression(v))
                }),
            )),
        ),
        space0,
    )(i)
}

fn select_clause_expression_list(i: &str) -> IResult<&str, ast::SelectClause, nom::error::Error<&str>> {
    map(select_expression_list, |v| SelectClause::SelectExpressions(v))(i)
}

pub(crate) fn select_query(i: &str) -> IResult<&str, ast::SelectStatement, nom::error::Error<&str>> {
    let (i, _) = tag_no_case("select")(i)?;
    let (i, _) = multispace1(i)?;
    let (i, distinct) = opt(|i| {
        let (i, _) = tag_no_case("distinct")(i)?;
        let (i, _) = multispace1(i)?;
        Ok((i, true))
    })(i)?;
    let distinct = distinct.unwrap_or(false);

    let (i, (select_clause, from_cl, where_expr, group_by_expr, having_expr, order_by_expr, limit_expr)) =
        tuple((
            alt((value_constructor, select_clause_expression_list)),
            from_clause,
            opt(where_expression),
            opt(group_by_expression),
            opt(having_expression),
            opt(order_by_clause),
            opt(limit_expression),
        ))(i)?;

    Ok((
        i,
        ast::SelectStatement::new(
            distinct,
            select_clause,
            from_cl,
            where_expr,
            group_by_expr,
            having_expr,
            order_by_expr,
            limit_expr,
        ),
    ))
}

fn set_operator(i: &str) -> IResult<&str, (ast::SetOperator, bool), nom::error::Error<&str>> {
    alt((
        map(
            tuple((tag_no_case("union"), multispace1, tag_no_case("all"))),
            |_| (ast::SetOperator::Union, true),
        ),
        map(tag_no_case("union"), |_| (ast::SetOperator::Union, false)),
        map(
            tuple((tag_no_case("intersect"), multispace1, tag_no_case("all"))),
            |_| (ast::SetOperator::Intersect, true),
        ),
        map(tag_no_case("intersect"), |_| {
            (ast::SetOperator::Intersect, false)
        }),
        map(
            tuple((tag_no_case("except"), multispace1, tag_no_case("all"))),
            |_| (ast::SetOperator::Except, true),
        ),
        map(tag_no_case("except"), |_| (ast::SetOperator::Except, false)),
    ))(i)
}

pub fn query(i: &str) -> IResult<&str, ast::Query, nom::error::Error<&str>> {
    let (i, first) = select_query(i)?;
    let mut result = ast::Query::Select(first);

    // Parse trailing UNION/INTERSECT/EXCEPT
    let mut remaining = i;
    loop {
        let (i2, _) = multispace0(remaining)?;
        if let Ok((i3, op)) = set_operator(i2) {
            let (i4, _) = multispace1(i3)?;
            let (i5, right_select) = select_query(i4)?;
            let (op_type, all) = op;
            result = ast::Query::SetOp {
                op: op_type,
                all,
                left: Box::new(result),
                right: Box::new(ast::Query::Select(right_select)),
            };
            remaining = i5;
        } else {
            break;
        }
    }

    Ok((remaining, result))
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_identifier() {
        assert_eq!(
            identifier::<nom::error::Error<&str>>("true"),
            Err(nom::Err::Failure(nom::error::Error::new("", nom::error::ErrorKind::Alpha)))
        );
        assert_eq!(
            identifier::<nom::error::Error<&str>>("false"),
            Err(nom::Err::Failure(nom::error::Error::new("", nom::error::ErrorKind::Alpha)))
        );
        assert_eq!(
            identifier::<nom::error::Error<&str>>("select"),
            Err(nom::Err::Failure(nom::error::Error::new("", nom::error::ErrorKind::Alpha)))
        );
        assert_eq!(
            identifier::<nom::error::Error<&str>>("order"),
            Err(nom::Err::Failure(nom::error::Error::new("", nom::error::ErrorKind::Alpha)))
        );
        assert_eq!(
            identifier::<nom::error::Error<&str>>("____"),
            Err(nom::Err::Failure(nom::error::Error::new("", nom::error::ErrorKind::Alpha)))
        );
        assert_eq!(
            identifier::<nom::error::Error<&str>>("123abc"),
            Err(nom::Err::Failure(nom::error::Error::new("", nom::error::ErrorKind::Alpha)))
        );
        assert_eq!(identifier::<nom::error::Error<&str>>("abc_fef"), Ok(("", "abc_fef")));
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
            branches: vec![(
                ast::Expression::Value(ast::Value::Boolean(true)),
                ast::Expression::Value(ast::Value::Integral(1)),
            )],
            else_expr: Some(Box::new(ast::Expression::Value(ast::Value::Integral(2)))),
        };

        assert_eq!(case_when_expression("case when true then 1 else 2 end"), Ok(("", ans)));
    }

    #[test]
    fn test_multi_branch_case_when() {
        let result = case_when_expression("case when true then 1 when false then 2 else 3 end");
        assert!(result.is_ok());
        let (_, cwe) = result.unwrap();
        assert_eq!(cwe.branches.len(), 2);
    }

    #[test]
    fn test_case_when_no_else() {
        let result = case_when_expression("case when true then 1 when false then 2 end");
        assert!(result.is_ok());
        let (_, cwe) = result.unwrap();
        assert_eq!(cwe.branches.len(), 2);
        assert!(cwe.else_expr.is_none());
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

        let select_expr = ast::Expression::Column(path_expr_a.clone());
        let where_expr = ast::WhereExpression::new(ast::Expression::BinaryOperator(
            ast::BinaryOperator::Equal,
            Box::new(ast::Expression::Column(path_expr_a.clone())),
            Box::new(ast::Expression::Value(ast::Value::Integral(1))),
        ));
        let path_expr = PathExpr::new(vec![PathSegment::AttrName("it".to_string())]);
        let table_reference = ast::TableReference::new(path_expr, None, None);
        let ans = ast::SelectStatement::new(
            false,
            SelectClause::ValueConstructor(ast::ValueConstructor::Expression(select_expr)),
            FromClause::Tables(vec![table_reference]),
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
            SelectClause::SelectExpressions(select_exprs),
            FromClause::Tables(vec![table_reference]),
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

        let group_by_ref_a = ast::GroupByReference::new(ast::Expression::Column(path_expr_a.clone()), None);
        let group_by_ref_b = ast::GroupByReference::new(ast::Expression::Column(path_expr_b.clone()), None);
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
            SelectClause::SelectExpressions(select_exprs),
            FromClause::Tables(vec![table_reference]),
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
            SelectClause::SelectExpressions(select_exprs),
            FromClause::Tables(vec![table_reference]),
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
            SelectClause::SelectExpressions(select_exprs),
            FromClause::Tables(vec![table_reference]),
            None,
            None,
            None,
            None,
            Some(limit_expr),
        );

        assert_eq!(select_query("select a, b, c from it limit 1"), Ok(("", ans)));
    }

    #[test]
    fn test_ordering_term() {
        let path_expr_a = PathExpr::new(vec![PathSegment::AttrName("a".to_string())]);
        let expected = ast::OrderingTerm::new(path_expr_a.clone(), "asc");

        assert_eq!(ordering_term("a asc"), Ok(("", expected)));
    }

    #[test]
    fn test_order_by_clause() {
        let path_expr_a = PathExpr::new(vec![PathSegment::AttrName("a".to_string())]);
        let expected = ast::OrderByExpression::new(vec![ast::OrderingTerm::new(path_expr_a.clone(), "asc")]);

        assert_eq!(order_by_clause("order by a asc"), Ok(("", expected)));
    }

    #[test]
    fn test_group_by_expression() {
        let path_expr_a = PathExpr::new(vec![PathSegment::AttrName("a".to_string())]);

        let func_call_expr = ast::Expression::FuncCall(
            "time_bucket".to_string(),
            vec![
                ast::SelectExpression::Expression(
                    Box::new(ast::Expression::Value(ast::Value::StringLiteral(
                        "5 seconds".to_string(),
                    ))),
                    None,
                ),
                ast::SelectExpression::Expression(Box::new(ast::Expression::Column(path_expr_a.clone())), None),
            ],
            None,
        );

        let group_by_ref_a = ast::GroupByReference::new(func_call_expr, Some("t".to_string()));
        let expected = ast::GroupByExpression::new(vec![group_by_ref_a], None);

        assert_eq!(
            group_by_expression("group by time_bucket(\"5 seconds\", a) as t "),
            Ok(("", expected))
        );
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

        let order_by_clause = ast::OrderByExpression::new(vec![ast::OrderingTerm::new(path_expr_a.clone(), "asc")]);
        let path_expr = PathExpr::new(vec![PathSegment::AttrName("it".to_string())]);
        let table_reference = ast::TableReference::new(path_expr, None, None);
        let ans = ast::SelectStatement::new(
            false,
            SelectClause::SelectExpressions(select_exprs),
            FromClause::Tables(vec![table_reference]),
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
        let path_expr_b = PathExpr::new(vec![PathSegment::AttrName("b".to_string())]);
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
                        ast::OrderingTerm::new(path_expr_b.clone(), "asc"),
                    ]))),
                )),
                None,
            ),
        ];

        let group_by_ref_a = ast::GroupByReference::new(ast::Expression::Column(path_expr_a.clone()), None);
        let group_by_ref_c = ast::GroupByReference::new(ast::Expression::Column(path_expr_c.clone()), None);
        let group_by_expr = ast::GroupByExpression::new(vec![group_by_ref_a, group_by_ref_c], None);

        let path_expr = PathExpr::new(vec![PathSegment::AttrName("it".to_string())]);
        let table_reference = ast::TableReference::new(path_expr, None, None);
        let ans = ast::SelectStatement::new(
            false,
            SelectClause::SelectExpressions(select_exprs),
            FromClause::Tables(vec![table_reference]),
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
        let ans = ast::SelectStatement::new(
            false,
            SelectClause::SelectExpressions(select_exprs),
            FromClause::Tables(vec![table_reference]),
            None,
            None,
            None,
            None,
            None,
        );
        assert_eq!(
            select_query("select a as aa, foo( b ) as bb, 1+1 as cc from it"),
            Ok(("", ans))
        );
    }

    #[test]
    fn test_select_stmt_error() {
        assert_eq!(
            select_query("select select from it"),
            Err(nom::Err::Failure(nom::error::Error::new(" from it", nom::error::ErrorKind::Alpha)))
        );

        assert_eq!(
            select_query("select * from it where limit 1"),
            Err(nom::Err::Failure(nom::error::Error::new(" 1", nom::error::ErrorKind::Alpha)))
        );
    }

    #[test]
    fn test_select_distinct_parsing() {
        let result = select_query("select distinct a from it");
        assert!(result.is_ok());
        let (_, stmt) = result.unwrap();
        assert!(stmt.distinct);
    }

    #[test]
    fn test_select_without_distinct() {
        let result = select_query("select a from it");
        assert!(result.is_ok());
        let (_, stmt) = result.unwrap();
        assert!(!stmt.distinct);
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
        let mut precedence_table: HashMap<&str, (u32, bool)> = HashMap::new();
        precedence_table.insert("*", (7, true));
        precedence_table.insert("/", (7, true));
        precedence_table.insert("+", (6, true));
        precedence_table.insert("-", (6, true));
        precedence_table.insert("||", (6, true));
        precedence_table.insert("<", (4, true));
        precedence_table.insert("<=", (4, true));
        precedence_table.insert(">", (4, true));
        precedence_table.insert(">=", (4, true));
        precedence_table.insert("=", (3, true));
        precedence_table.insert("!=", (3, true));
        precedence_table.insert("and", (2, true));
        precedence_table.insert("or", (1, true));
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
    fn test_wildcard_array_path() {
        let result = path_expr("a[*]");
        assert!(result.is_ok());
        let (_, pe) = result.unwrap();
        assert_eq!(pe.path_segments.len(), 2);
        assert_eq!(pe.path_segments[0], ast::PathSegment::AttrName("a".to_string()));
        assert_eq!(pe.path_segments[1], ast::PathSegment::Wildcard);
    }

    #[test]
    fn test_wildcard_attr_path() {
        let result = path_expr("a.*");
        assert!(result.is_ok());
        let (_, pe) = result.unwrap();
        assert_eq!(pe.path_segments.len(), 2);
        assert_eq!(pe.path_segments[0], ast::PathSegment::AttrName("a".to_string()));
        assert_eq!(pe.path_segments[1], ast::PathSegment::WildcardAttr);
    }

    #[test]
    fn test_wildcard_array_path_nested() {
        let result = path_expr("a.b[*].c");
        assert!(result.is_ok());
        let (_, pe) = result.unwrap();
        assert_eq!(pe.path_segments.len(), 4);
        assert_eq!(pe.path_segments[0], ast::PathSegment::AttrName("a".to_string()));
        assert_eq!(pe.path_segments[1], ast::PathSegment::AttrName("b".to_string()));
        assert_eq!(pe.path_segments[2], ast::PathSegment::Wildcard);
        assert_eq!(pe.path_segments[3], ast::PathSegment::AttrName("c".to_string()));
    }

    #[test]
    fn test_wildcard_attr_path_nested() {
        let result = path_expr("a.b.*");
        assert!(result.is_ok());
        let (_, pe) = result.unwrap();
        assert_eq!(pe.path_segments.len(), 3);
        assert_eq!(pe.path_segments[0], ast::PathSegment::AttrName("a".to_string()));
        assert_eq!(pe.path_segments[1], ast::PathSegment::AttrName("b".to_string()));
        assert_eq!(pe.path_segments[2], ast::PathSegment::WildcardAttr);
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

    #[test]
    fn test_value_constructor() {
        let (_, ans) = value_constructor(" value [a, b]").unwrap();
        let values = vec![
            ast::Expression::Column(ast::PathExpr::new(vec![ast::PathSegment::AttrName("a".to_string())])),
            ast::Expression::Column(ast::PathExpr::new(vec![ast::PathSegment::AttrName("b".to_string())])),
        ];

        let expected =
            ast::SelectClause::ValueConstructor(ast::ValueConstructor::ArrayConstructor(ast::ArrayConstructor {
                values,
            }));
        assert_eq!(expected, ans);

        let (_, ans) = value_constructor(" value a").unwrap();
        let expr = ast::Expression::Column(ast::PathExpr::new(vec![ast::PathSegment::AttrName("a".to_string())]));
        let expected = ast::SelectClause::ValueConstructor(ast::ValueConstructor::Expression(expr));
        assert_eq!(expected, ans);
    }

    #[test]
    fn test_mixed_case_query() {
        let result = select_query("SELECT a FROM it WHERE a = 1");
        assert!(result.is_ok());
    }

    #[test]
    fn test_string_literal_case_preserved() {
        let result = select_query("select a from it where a = \"Alice\"");
        match result {
            Ok((_, stmt)) => {
                if let Some(where_expr) = stmt.where_expr_opt {
                    match where_expr.expr {
                        ast::Expression::BinaryOperator(_, _, right) => match *right {
                            ast::Expression::Value(ast::Value::StringLiteral(s)) => {
                                assert_eq!(s, "Alice");
                            }
                            _ => panic!("Expected string literal"),
                        },
                        _ => panic!("Expected binary operator"),
                    }
                }
            }
            Err(e) => panic!("Parse failed: {:?}", e),
        }
    }

    #[test]
    fn test_keyword_as_identifier_rejected_case_insensitive() {
        assert!(identifier::<nom::error::Error<&str>>("SELECT").is_err());
        assert!(identifier::<nom::error::Error<&str>>("From").is_err());
        assert!(identifier::<nom::error::Error<&str>>("WHERE").is_err());
        assert!(identifier::<nom::error::Error<&str>>("having").is_err());
    }

    #[test]
    fn test_uppercase_logical_operators() {
        let result = select_query("SELECT a FROM it WHERE a = 1 AND b = 2");
        assert!(result.is_ok(), "Uppercase AND should parse successfully, got: {:?}", result);
        let result = select_query("SELECT a FROM it WHERE a = 1 OR b = 2");
        assert!(result.is_ok(), "Uppercase OR should parse successfully, got: {:?}", result);
    }

    #[test]
    fn test_null_literal() {
        let result = expression("null");
        assert!(result.is_ok(), "null literal should parse, got: {:?}", result);
        let (_, expr) = result.unwrap();
        assert_eq!(expr, ast::Expression::Value(ast::Value::Null));
    }

    #[test]
    fn test_null_literal_uppercase() {
        let result = expression("NULL");
        assert!(result.is_ok(), "NULL literal should parse, got: {:?}", result);
        let (_, expr) = result.unwrap();
        assert_eq!(expr, ast::Expression::Value(ast::Value::Null));
    }

    #[test]
    fn test_missing_literal() {
        let result = expression("missing");
        assert!(result.is_ok(), "missing literal should parse, got: {:?}", result);
        let (_, expr) = result.unwrap();
        assert_eq!(expr, ast::Expression::Value(ast::Value::Missing));
    }

    #[test]
    fn test_is_null_parsing() {
        let result = select_query("select a from it where a is null");
        assert!(result.is_ok(), "IS NULL should parse, got: {:?}", result);

        let result = select_query("select a from it where a IS NULL");
        assert!(result.is_ok(), "IS NULL uppercase should parse, got: {:?}", result);
    }

    #[test]
    fn test_is_not_null_parsing() {
        let result = select_query("select a from it where a is not null");
        assert!(result.is_ok(), "IS NOT NULL should parse, got: {:?}", result);
    }

    #[test]
    fn test_is_missing_parsing() {
        let result = select_query("select a from it where a is missing");
        assert!(result.is_ok(), "IS MISSING should parse, got: {:?}", result);
    }

    #[test]
    fn test_is_not_missing_parsing() {
        let result = select_query("select a from it where a is not missing");
        assert!(result.is_ok(), "IS NOT MISSING should parse, got: {:?}", result);
    }

    #[test]
    fn test_is_null_expression_structure() {
        let path_expr_a = PathExpr::new(vec![PathSegment::AttrName("a".to_string())]);
        let expected = ast::Expression::IsNull(Box::new(ast::Expression::Column(path_expr_a)));
        let result = expression("a is null");
        assert!(result.is_ok(), "a is null should parse, got: {:?}", result);
        let (_, expr) = result.unwrap();
        assert_eq!(expr, expected);
    }

    #[test]
    fn test_is_not_null_expression_structure() {
        let path_expr_a = PathExpr::new(vec![PathSegment::AttrName("a".to_string())]);
        let expected = ast::Expression::IsNotNull(Box::new(ast::Expression::Column(path_expr_a)));
        let result = expression("a is not null");
        assert!(result.is_ok(), "a is not null should parse, got: {:?}", result);
        let (_, expr) = result.unwrap();
        assert_eq!(expr, expected);
    }

    #[test]
    fn test_is_missing_expression_structure() {
        let path_expr_a = PathExpr::new(vec![PathSegment::AttrName("a".to_string())]);
        let expected = ast::Expression::IsMissing(Box::new(ast::Expression::Column(path_expr_a)));
        let result = expression("a is missing");
        assert!(result.is_ok(), "a is missing should parse, got: {:?}", result);
        let (_, expr) = result.unwrap();
        assert_eq!(expr, expected);
    }

    #[test]
    fn test_is_not_missing_expression_structure() {
        let path_expr_a = PathExpr::new(vec![PathSegment::AttrName("a".to_string())]);
        let expected = ast::Expression::IsNotMissing(Box::new(ast::Expression::Column(path_expr_a)));
        let result = expression("a is not missing");
        assert!(result.is_ok(), "a is not missing should parse, got: {:?}", result);
        let (_, expr) = result.unwrap();
        assert_eq!(expr, expected);
    }

    #[test]
    fn test_string_concat_parsing() {
        let result = expression("a || b");
        assert!(result.is_ok(), "|| should parse, got: {:?}", result);
        let (_, expr) = result.unwrap();
        let path_expr_a = PathExpr::new(vec![PathSegment::AttrName("a".to_string())]);
        let path_expr_b = PathExpr::new(vec![PathSegment::AttrName("b".to_string())]);
        let expected = ast::Expression::BinaryOperator(
            ast::BinaryOperator::Concat,
            Box::new(ast::Expression::Column(path_expr_a)),
            Box::new(ast::Expression::Column(path_expr_b)),
        );
        assert_eq!(expr, expected);
    }

    #[test]
    fn test_string_concat_chained() {
        let result = expression("a || b || c");
        assert!(result.is_ok(), "chained || should parse, got: {:?}", result);
        let (_, expr) = result.unwrap();
        // Left-associative: (a || b) || c
        let path_expr_a = PathExpr::new(vec![PathSegment::AttrName("a".to_string())]);
        let path_expr_b = PathExpr::new(vec![PathSegment::AttrName("b".to_string())]);
        let path_expr_c = PathExpr::new(vec![PathSegment::AttrName("c".to_string())]);
        let expected = ast::Expression::BinaryOperator(
            ast::BinaryOperator::Concat,
            Box::new(ast::Expression::BinaryOperator(
                ast::BinaryOperator::Concat,
                Box::new(ast::Expression::Column(path_expr_a)),
                Box::new(ast::Expression::Column(path_expr_b)),
            )),
            Box::new(ast::Expression::Column(path_expr_c)),
        );
        assert_eq!(expr, expected);
    }

    #[test]
    fn test_like_parsing() {
        let result = select_query("select a from it where a like \"%foo%\"");
        assert!(result.is_ok(), "LIKE should parse, got: {:?}", result);
    }

    #[test]
    fn test_not_like_parsing() {
        let result = select_query("select a from it where a not like \"%foo%\"");
        assert!(result.is_ok(), "NOT LIKE should parse, got: {:?}", result);
    }

    #[test]
    fn test_like_expression_structure() {
        let path_expr_a = PathExpr::new(vec![PathSegment::AttrName("a".to_string())]);
        let expected = ast::Expression::Like(
            Box::new(ast::Expression::Column(path_expr_a)),
            Box::new(ast::Expression::Value(ast::Value::StringLiteral("%foo%".to_string()))),
        );
        let result = expression("a like \"%foo%\"");
        assert!(result.is_ok(), "a like should parse, got: {:?}", result);
        let (_, expr) = result.unwrap();
        assert_eq!(expr, expected);
    }

    #[test]
    fn test_not_like_expression_structure() {
        let path_expr_a = PathExpr::new(vec![PathSegment::AttrName("a".to_string())]);
        let expected = ast::Expression::NotLike(
            Box::new(ast::Expression::Column(path_expr_a)),
            Box::new(ast::Expression::Value(ast::Value::StringLiteral("%foo%".to_string()))),
        );
        let result = expression("a not like \"%foo%\"");
        assert!(result.is_ok(), "a not like should parse, got: {:?}", result);
        let (_, expr) = result.unwrap();
        assert_eq!(expr, expected);
    }

    #[test]
    fn test_like_case_insensitive_keyword() {
        let result = select_query("select a from it where a LIKE \"%foo%\"");
        assert!(result.is_ok(), "LIKE uppercase should parse, got: {:?}", result);
        let result = select_query("select a from it where a NOT LIKE \"%foo%\"");
        assert!(result.is_ok(), "NOT LIKE uppercase should parse, got: {:?}", result);
    }

    #[test]
    fn test_like_with_and() {
        // LIKE should work with AND/OR
        let result = select_query("select a from it where a like \"%foo%\" and b = 1");
        assert!(result.is_ok(), "LIKE with AND should parse, got: {:?}", result);
    }

    #[test]
    fn test_between_parsing() {
        let result = select_query("select a from it where a between 1 and 10");
        assert!(result.is_ok(), "BETWEEN should parse, got: {:?}", result);
    }

    #[test]
    fn test_not_between_parsing() {
        let result = select_query("select a from it where a not between 1 and 10");
        assert!(result.is_ok(), "NOT BETWEEN should parse, got: {:?}", result);
    }

    #[test]
    fn test_between_case_insensitive() {
        let result = select_query("SELECT a FROM it WHERE a BETWEEN 1 AND 10");
        assert!(result.is_ok(), "BETWEEN uppercase should parse, got: {:?}", result);
    }

    #[test]
    fn test_between_expression_structure() {
        let path_expr_a = PathExpr::new(vec![PathSegment::AttrName("a".to_string())]);
        let expected = ast::Expression::Between(
            Box::new(ast::Expression::Column(path_expr_a)),
            Box::new(ast::Expression::Value(ast::Value::Integral(1))),
            Box::new(ast::Expression::Value(ast::Value::Integral(10))),
        );
        let result = expression("a between 1 and 10");
        assert!(result.is_ok(), "a between 1 and 10 should parse, got: {:?}", result);
        let (_, expr) = result.unwrap();
        assert_eq!(expr, expected);
    }

    #[test]
    fn test_not_between_expression_structure() {
        let path_expr_a = PathExpr::new(vec![PathSegment::AttrName("a".to_string())]);
        let expected = ast::Expression::NotBetween(
            Box::new(ast::Expression::Column(path_expr_a)),
            Box::new(ast::Expression::Value(ast::Value::Integral(1))),
            Box::new(ast::Expression::Value(ast::Value::Integral(10))),
        );
        let result = expression("a not between 1 and 10");
        assert!(result.is_ok(), "a not between 1 and 10 should parse, got: {:?}", result);
        let (_, expr) = result.unwrap();
        assert_eq!(expr, expected);
    }

    #[test]
    fn test_between_with_logical_and() {
        // The AND inside BETWEEN should not be consumed as a logical AND
        let result = select_query("select a from it where a between 1 and 10 and b = 1");
        assert!(result.is_ok(), "BETWEEN with trailing AND should parse, got: {:?}", result);
    }

    #[test]
    fn test_in_parsing() {
        let result = select_query("select a from it where a in (1, 2, 3)");
        assert!(result.is_ok(), "IN should parse, got: {:?}", result);
    }

    #[test]
    fn test_not_in_parsing() {
        let result = select_query("select a from it where a not in (1, 2, 3)");
        assert!(result.is_ok(), "NOT IN should parse, got: {:?}", result);
    }

    #[test]
    fn test_in_expression_structure() {
        let path_expr_a = PathExpr::new(vec![PathSegment::AttrName("a".to_string())]);
        let expected = ast::Expression::In(
            Box::new(ast::Expression::Column(path_expr_a)),
            vec![
                ast::Expression::Value(ast::Value::Integral(1)),
                ast::Expression::Value(ast::Value::Integral(2)),
                ast::Expression::Value(ast::Value::Integral(3)),
            ],
        );
        let result = expression("a in (1, 2, 3)");
        assert!(result.is_ok(), "a in (1, 2, 3) should parse, got: {:?}", result);
        let (_, expr) = result.unwrap();
        assert_eq!(expr, expected);
    }

    #[test]
    fn test_not_in_expression_structure() {
        let path_expr_a = PathExpr::new(vec![PathSegment::AttrName("a".to_string())]);
        let expected = ast::Expression::NotIn(
            Box::new(ast::Expression::Column(path_expr_a)),
            vec![
                ast::Expression::Value(ast::Value::Integral(1)),
                ast::Expression::Value(ast::Value::Integral(2)),
                ast::Expression::Value(ast::Value::Integral(3)),
            ],
        );
        let result = expression("a not in (1, 2, 3)");
        assert!(result.is_ok(), "a not in (1, 2, 3) should parse, got: {:?}", result);
        let (_, expr) = result.unwrap();
        assert_eq!(expr, expected);
    }

    #[test]
    fn test_in_case_insensitive() {
        let result = select_query("select a from it where a IN (1, 2)");
        assert!(result.is_ok(), "IN uppercase should parse, got: {:?}", result);
        let result = select_query("select a from it where a NOT IN (1, 2)");
        assert!(result.is_ok(), "NOT IN uppercase should parse, got: {:?}", result);
    }

    #[test]
    fn test_in_with_and() {
        let result = select_query("select a from it where a in (1, 2, 3) and b = 1");
        assert!(result.is_ok(), "IN with AND should parse, got: {:?}", result);
    }

    #[test]
    fn test_coalesce_parsing() {
        // coalesce parses as a FuncCall through the existing func_call parser
        let result = expression("coalesce(a, b)");
        assert!(result.is_ok(), "coalesce(a, b) should parse, got: {:?}", result);
        let (_, expr) = result.unwrap();
        match expr {
            ast::Expression::FuncCall(name, args, _) => {
                assert_eq!(name, "coalesce");
                assert_eq!(args.len(), 2);
            }
            other => panic!("Expected FuncCall, got {:?}", other),
        }
    }

    #[test]
    fn test_coalesce_three_args_parsing() {
        let result = expression("coalesce(a, b, c)");
        assert!(result.is_ok(), "coalesce(a, b, c) should parse, got: {:?}", result);
        let (_, expr) = result.unwrap();
        match expr {
            ast::Expression::FuncCall(name, args, _) => {
                assert_eq!(name, "coalesce");
                assert_eq!(args.len(), 3);
            }
            other => panic!("Expected FuncCall, got {:?}", other),
        }
    }

    #[test]
    fn test_nullif_parsing() {
        let result = expression("nullif(a, b)");
        assert!(result.is_ok(), "nullif(a, b) should parse, got: {:?}", result);
        let (_, expr) = result.unwrap();
        match expr {
            ast::Expression::FuncCall(name, args, _) => {
                assert_eq!(name, "nullif");
                assert_eq!(args.len(), 2);
            }
            other => panic!("Expected FuncCall, got {:?}", other),
        }
    }

    #[test]
    fn test_coalesce_in_select() {
        let result = select_query("select coalesce(a, b) from it");
        assert!(result.is_ok(), "COALESCE in SELECT should parse, got: {:?}", result);
    }

    #[test]
    fn test_nullif_in_select() {
        let result = select_query("select nullif(a, b) from it");
        assert!(result.is_ok(), "NULLIF in SELECT should parse, got: {:?}", result);
    }

    #[test]
    fn test_coalesce_in_where() {
        let result = select_query("select a from it where coalesce(a, 0) > 1");
        assert!(result.is_ok(), "COALESCE in WHERE should parse, got: {:?}", result);
    }

    #[test]
    fn test_nullif_in_where() {
        let result = select_query("select a from it where nullif(a, 0) > 1");
        assert!(result.is_ok(), "NULLIF in WHERE should parse, got: {:?}", result);
    }

    #[test]
    fn test_cast_parsing() {
        let result = expression("cast(a as int)");
        assert!(result.is_ok(), "cast(a as int) should parse, got: {:?}", result);
        let (_, expr) = result.unwrap();
        let path_expr_a = PathExpr::new(vec![PathSegment::AttrName("a".to_string())]);
        let expected = ast::Expression::Cast(
            Box::new(ast::Expression::Column(path_expr_a)),
            ast::CastType::Int,
        );
        assert_eq!(expr, expected);
    }

    #[test]
    fn test_cast_parsing_uppercase() {
        let result = expression("CAST(a AS VARCHAR)");
        assert!(result.is_ok(), "CAST(a AS VARCHAR) should parse, got: {:?}", result);
        let (_, expr) = result.unwrap();
        let path_expr_a = PathExpr::new(vec![PathSegment::AttrName("a".to_string())]);
        let expected = ast::Expression::Cast(
            Box::new(ast::Expression::Column(path_expr_a)),
            ast::CastType::Varchar,
        );
        assert_eq!(expr, expected);
    }

    #[test]
    fn test_cast_float_type() {
        let result = expression("cast(a as float)");
        assert!(result.is_ok(), "cast(a as float) should parse, got: {:?}", result);
        let (_, expr) = result.unwrap();
        match expr {
            ast::Expression::Cast(_, ast::CastType::Float) => {}
            other => panic!("Expected Cast with Float, got {:?}", other),
        }
    }

    #[test]
    fn test_cast_boolean_type() {
        let result = expression("cast(a as boolean)");
        assert!(result.is_ok(), "cast(a as boolean) should parse, got: {:?}", result);
        let (_, expr) = result.unwrap();
        match expr {
            ast::Expression::Cast(_, ast::CastType::Boolean) => {}
            other => panic!("Expected Cast with Boolean, got {:?}", other),
        }
    }

    #[test]
    fn test_cast_bool_alias() {
        let result = expression("cast(a as bool)");
        assert!(result.is_ok(), "cast(a as bool) should parse, got: {:?}", result);
        let (_, expr) = result.unwrap();
        match expr {
            ast::Expression::Cast(_, ast::CastType::Boolean) => {}
            other => panic!("Expected Cast with Boolean, got {:?}", other),
        }
    }

    #[test]
    fn test_cast_integer_alias() {
        let result = expression("cast(a as integer)");
        assert!(result.is_ok(), "cast(a as integer) should parse, got: {:?}", result);
        let (_, expr) = result.unwrap();
        match expr {
            ast::Expression::Cast(_, ast::CastType::Int) => {}
            other => panic!("Expected Cast with Int, got {:?}", other),
        }
    }

    #[test]
    fn test_cast_string_alias() {
        let result = expression("cast(a as string)");
        assert!(result.is_ok(), "cast(a as string) should parse, got: {:?}", result);
        let (_, expr) = result.unwrap();
        match expr {
            ast::Expression::Cast(_, ast::CastType::Varchar) => {}
            other => panic!("Expected Cast with Varchar, got {:?}", other),
        }
    }

    #[test]
    fn test_cast_in_select() {
        let result = select_query("select cast(a as int) from it");
        assert!(result.is_ok(), "CAST in SELECT should parse, got: {:?}", result);
    }

    #[test]
    fn test_cast_in_where() {
        let result = select_query("select a from it where cast(a as int) > 1");
        assert!(result.is_ok(), "CAST in WHERE should parse, got: {:?}", result);
    }

    #[test]
    fn test_cast_with_expression() {
        let result = expression("cast(a + b as varchar)");
        assert!(result.is_ok(), "cast(a + b as varchar) should parse, got: {:?}", result);
    }

    #[test]
    fn test_cross_join_parsing() {
        let result = select_query("select a.x, b.y from it as a cross join it as b");
        assert!(result.is_ok(), "CROSS JOIN should parse, got: {:?}", result);
        let (_, stmt) = result.unwrap();
        match &stmt.from_clause {
            FromClause::Tables(refs) => {
                assert_eq!(refs.len(), 2);
                assert_eq!(refs[0].as_clause, Some("a".to_string()));
                assert_eq!(refs[1].as_clause, Some("b".to_string()));
            }
            other => panic!("Expected FromClause::Tables, got: {:?}", other),
        }
    }

    #[test]
    fn test_cross_join_case_insensitive() {
        let result = select_query("select a.x from it as a CROSS JOIN it as b");
        assert!(result.is_ok(), "CROSS JOIN (uppercase) should parse, got: {:?}", result);
        let (_, stmt) = result.unwrap();
        match &stmt.from_clause {
            FromClause::Tables(refs) => assert_eq!(refs.len(), 2),
            other => panic!("Expected FromClause::Tables, got: {:?}", other),
        }
    }

    #[test]
    fn test_cross_join_and_comma_both_work() {
        // Comma-separated FROM items (implicit cross join)
        let result1 = select_query("select a.x from it as a, it as b");
        assert!(result1.is_ok(), "Comma-separated FROM should parse, got: {:?}", result1);
        let (_, stmt1) = result1.unwrap();
        match &stmt1.from_clause {
            FromClause::Tables(refs) => assert_eq!(refs.len(), 2),
            other => panic!("Expected FromClause::Tables, got: {:?}", other),
        }

        // Explicit CROSS JOIN
        let result2 = select_query("select a.x from it as a cross join it as b");
        assert!(result2.is_ok(), "CROSS JOIN should parse, got: {:?}", result2);
        let (_, stmt2) = result2.unwrap();
        match &stmt2.from_clause {
            FromClause::Tables(refs) => assert_eq!(refs.len(), 2),
            other => panic!("Expected FromClause::Tables, got: {:?}", other),
        }
    }

    #[test]
    fn test_cross_join_with_where() {
        let result = select_query("select a.x, b.y from it as a cross join it as b where a.x = b.y");
        assert!(result.is_ok(), "CROSS JOIN with WHERE should parse, got: {:?}", result);
    }

    #[test]
    fn test_cross_join_three_way() {
        let result = select_query("select * from it as a cross join it as b cross join it as c");
        assert!(result.is_ok(), "Three-way CROSS JOIN should parse, got: {:?}", result);
        let (_, stmt) = result.unwrap();
        match &stmt.from_clause {
            FromClause::Tables(refs) => assert_eq!(refs.len(), 3),
            other => panic!("Expected FromClause::Tables, got: {:?}", other),
        }
    }

    #[test]
    fn test_left_join_parsing() {
        let result = select_query("select a.x, b.x from it as a left join it as b on a.x = b.x");
        assert!(result.is_ok(), "LEFT JOIN should parse, got: {:?}", result);
        let (_, stmt) = result.unwrap();
        match &stmt.from_clause {
            FromClause::Join { join_type, condition, .. } => {
                assert_eq!(*join_type, JoinType::Left);
                assert!(condition.is_some());
            }
            other => panic!("Expected FromClause::Join, got: {:?}", other),
        }
    }

    #[test]
    fn test_left_outer_join_parsing() {
        let result = select_query("select a.x, b.x from it as a left outer join it as b on a.x = b.x");
        assert!(result.is_ok(), "LEFT OUTER JOIN should parse, got: {:?}", result);
        let (_, stmt) = result.unwrap();
        match &stmt.from_clause {
            FromClause::Join { join_type, condition, .. } => {
                assert_eq!(*join_type, JoinType::Left);
                assert!(condition.is_some());
            }
            other => panic!("Expected FromClause::Join, got: {:?}", other),
        }
    }

    #[test]
    fn test_subquery_in_where() {
        let result = select_query("select a from it where a = (select max(a) from it)");
        assert!(result.is_ok(), "Subquery in WHERE should parse, got: {:?}", result);
    }

    #[test]
    fn test_subquery_in_select() {
        let result = select_query("select a, (select count(*) from it) as total from it");
        assert!(result.is_ok(), "Subquery in SELECT should parse, got: {:?}", result);
    }

    #[test]
    fn test_union_parsing() {
        let result = query("select a from it union select b from it");
        assert!(result.is_ok(), "UNION should parse, got: {:?}", result);
    }

    #[test]
    fn test_union_all_parsing() {
        let result = query("select a from it union all select b from it");
        assert!(result.is_ok(), "UNION ALL should parse, got: {:?}", result);
    }

    #[test]
    fn test_intersect_parsing() {
        let result = query("select a from it intersect select b from it");
        assert!(result.is_ok(), "INTERSECT should parse, got: {:?}", result);
    }

    #[test]
    fn test_except_parsing() {
        let result = query("select a from it except select b from it");
        assert!(result.is_ok(), "EXCEPT should parse, got: {:?}", result);
    }
}
