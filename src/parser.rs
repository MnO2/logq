use crate::ast;

type ParseError = String;
type ParseErrors = Vec<ParseError>;

pub type ParseResult<T> = Result<T, ParseError>;

pub fn parse(input: &str) -> Result<ast::Node, ParseErrors> {
    Ok(ast::Node::Field(Box::new(ast::Field { name: String::from("abc") })))
}