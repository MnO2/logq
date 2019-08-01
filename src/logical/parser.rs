use super::types::Node;

#[derive(Fail, Debug)]
pub enum ParseError {
    #[fail(display = "Unexpected Token Found")]
    UnexpecedToken(Token),
}

pub type ParseResult<T> = Result<T, ParseError>;

fn parse_query(query: Query) -> ParseResult<Box<dyn Node>> {
    let root: Box<dyn Node>;
    root
}