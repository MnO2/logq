use crate::ast;
use crate::lexer::Lexer;
use crate::token::Token;

type ParseError = String;
type ParseErrors = Vec<ParseError>;

pub type ParseResult<T> = Result<T, ParseError>;

pub fn parse(input: &str) -> Result<ast::Node, ParseErrors> {
    let l = Lexer::new(input);
    let mut p = Parser::new(l);
    let query = p.parse_query()?;

    Ok(ast::Node::Query(Box::new(query)))
}

pub struct Parser<'a> {
    lexer: Lexer<'a>,

    curr_token: Token,
    peek_token: Token
}

impl<'a> Parser<'a> {
    pub fn new(l: Lexer<'a>) -> Self {
        let mut l = l;
        let curr = l.next_token();
        let next = l.next_token();

        Parser {
            lexer: l, 
            curr_token: curr,
            peek_token: next
        }
    }

    pub fn parse_query(&mut self) -> Result<ast::Query, ParseErrors> {
        let mut query = ast::Query::new();
        let mut errors = ParseErrors::new();
        let mut tok = self.curr_token.clone();

        while tok != Token::EOF {
            match self.parse_field() {
                Ok(field) => { query.fields.push(field) },
                Err(err) => { errors.push(err) },
            }  

            self.next_token();
            tok = self.curr_token.clone();
        }

        if !errors.is_empty() {
            return Err(errors);
        }

        Ok(query)
    }

    pub fn parse_field(&mut self) -> ParseResult<ast::Field> {
        match &self.curr_token.clone() {
            Token::Ident(name) => { Ok(ast::Field { name: name.to_string() }) },
            _ => { Err("unexpected token".to_string()) }
        }
    }

    fn next_token(&mut self) {
        self.curr_token = self.peek_token.clone();
        self.peek_token = self.lexer.next_token();
    }
}
