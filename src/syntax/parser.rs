use super::ast;
use super::lexer::Lexer;
use super::token::Token;

#[derive(Fail, Debug)]
pub(crate) enum ParseError {
    #[fail(display = "Unexpected Token Found")]
    UnexpecedToken(Token),
}

pub(crate) type ParseResult<T> = Result<T, ParseError>;

pub(crate) fn parse(input: &str) -> ParseResult<ast::Node> {
    let l = Lexer::new(input);
    let mut p = Parser::new(l);
    let query = p.parse_query()?;

    Ok(ast::Node::Query(Box::new(query)))
}

pub(crate) struct Parser<'a> {
    lexer: Lexer<'a>,

    curr_token: Token,
    peek_token: Token,
}

impl<'a> Parser<'a> {
    pub(crate) fn new(l: Lexer<'a>) -> Self {
        let mut l = l;
        let curr = l.next_token();
        let next = l.next_token();

        Parser {
            lexer: l,
            curr_token: curr,
            peek_token: next,
        }
    }

    pub(crate) fn parse_query(&mut self) -> ParseResult<ast::Query> {
        let mut query = ast::Query::new();
        let fields = self.parse_select_expression_list()?;
        let where_clause = self.parse_where_clause()?;

        Ok(query)
    }

    pub(crate) fn parse_select_expression_list(&mut self) -> ParseResult<Vec<ast::SelectExpression>> {
        let mut select_exprs: Vec<ast::SelectExpression> = Vec::new();

        while self.expect_curr(&Token::EOF).is_err() {
            let select_expr = self.parse_select_expression()?;
            select_exprs.push(select_expr);

            if self.expect_curr(&Token::Comma).is_err() {
                break;
            }
        }

        Ok(select_exprs)
    }

    pub(crate) fn parse_select_expression(&mut self) -> ParseResult<ast::SelectExpression> {
        if self.expect_curr(&Token::Star).is_ok() {
            Ok(ast::SelectExpression::Star)
        } else {
            let expr = self.parse_expression()?;
            Ok(ast::SelectExpression::Expression(Box::new(expr)))
        }
    }

    pub(crate) fn parse_expression(&mut self) -> ParseResult<ast::Expression> {
        unimplemented!();
    }

    pub(crate) fn parse_value_expression(&mut self) -> ParseResult<ast::ValueExpression> {
        if let Token::Ident(ref name) = self.curr_token.clone() {
            self.next_token();
            return Ok(ast::ValueExpression::Column(name.to_string()));
        } else if let Token::Int(int) = self.curr_token.clone() {
            self.next_token();
            return Ok(ast::ValueExpression::Int(int));
        }

        Err(ParseError::UnexpecedToken(self.curr_token.clone()))
    }

    pub(crate) fn parse_where_clause(&mut self) -> ParseResult<ast::WhereClause> {
        unimplemented!();
    }

    pub(crate) fn parse_func_call(&mut self) -> ParseResult<ast::FuncCallExpression> {
        unimplemented!();
    }

    fn parse_identifier(&mut self) -> ParseResult<String> {
        if let Token::Ident(ref name) = self.curr_token.clone() {
            self.next_token();
            return Ok(name.to_string());
        }

        Err(ParseError::UnexpecedToken(self.curr_token.clone()))
    }

    fn expect_curr(&mut self, token: &Token) -> Result<(), ParseError> {
        if self.curr_token_is(&token) {
            self.next_token();
            Ok(())
        } else {
            Err(ParseError::UnexpecedToken(self.curr_token.clone()))
        }
    }

    fn curr_token_is(&self, token: &Token) -> bool {
        match (&token, &self.curr_token) {
            (Token::Ident(_), Token::Ident(_)) => true,
            (Token::Int(_), Token::Int(_)) => true,
            _ => token == &self.curr_token,
        }
    }

    fn next_token(&mut self) {
        self.curr_token = self.peek_token.clone();
        self.peek_token = self.lexer.next_token();
    }
}

#[cfg(test)]
mod test {
    use super::super::lexer::Lexer;
    use super::*;
}
