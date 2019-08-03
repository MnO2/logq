use super::types;
use crate::syntax::ast;

#[derive(Fail, Debug, Clone, PartialEq, Eq)]
pub enum ParseError {
    #[fail(display = "Type Mismatch")]
    TypeMismatch,
    #[fail(display = "Unsupported Logic Operator")]
    UnsupportedLogicOperator,
}

pub type ParseResult<T> = Result<T, ParseError>;

fn parse_prefix_operator(operator: &str, child: &ast::Expression) -> ParseResult<Box<dyn types::Formula>> {
    let child_parsed = parse_logic(child)?;

    let prefix_op = types::PrefixOperator::new(operator.to_string(), child_parsed);
    Ok(Box::new(prefix_op))
}

fn parse_infix_operator(
    operator: &str,
    left: &ast::Expression,
    right: &ast::Expression,
) -> ParseResult<Box<dyn types::Formula>> {
    let left_parsed = parse_logic(left)?;
    let right_parsed = parse_logic(right)?;

    let infix_op = types::InfixOperator::new(operator.to_string(), left_parsed, right_parsed);
    Ok(Box::new(infix_op))
}

fn parse_logic(expr: &ast::Expression) -> ParseResult<Box<dyn types::Formula>> {
    match expr {
        ast::Expression::And(l, r) => parse_infix_operator("AND", l, r),
        ast::Expression::Or(l, r) => parse_infix_operator("OR", l, r),
        ast::Expression::Not(c) => parse_prefix_operator("NOT", c),
        _ => Err(ParseError::UnsupportedLogicOperator),
    }
}

fn parse_logic_expression(expr: &ast::Expression) -> ParseResult<Box<dyn types::Expression>> {
    let formula = parse_logic(expr)?;
    Ok(Box::new(types::LogicExpression::new(formula)))
}

fn parse_expression(expr: &ast::Expression) -> ParseResult<Box<dyn types::Expression>> {
    match expr {
        ast::Expression::Condition(c) => {}
        ast::Expression::And(_, _) => {
            parse_logic_expression(expr);
        }
        ast::Expression::Or(_, _) => {
            parse_logic_expression(expr);
        }
        ast::Expression::Not(_) => {
            parse_logic_expression(expr);
        }
        ast::Expression::Value(v) => {}
    };
    let variable = types::Variable::new("a".to_string());

    Ok(Box::new(variable))
}

fn parse_query(query: ast::SelectStatement, data_source: types::DataSource) -> ParseResult<Box<dyn types::Node>> {
    let root: Box<dyn types::Node> = Box::new(data_source);

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

    Ok(root)
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_parse_variable() {}
}
