use crate::ast;
use crate::lexer::Lexer;
use crate::token::Token;

#[derive(Fail, Debug)]
pub enum ParseError {
    #[fail(display = "Unexpected Token Found")]
    UnexpecedToken(Token),
}

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
    peek_token: Token,
}

impl<'a> Parser<'a> {
    pub fn new(l: Lexer<'a>) -> Self {
        let mut l = l;
        let curr = l.next_token();
        let next = l.next_token();

        Parser {
            lexer: l,
            curr_token: curr,
            peek_token: next,
        }
    }

    pub fn parse_query(&mut self) -> Result<ast::Query, ParseErrors> {
        let mut query = ast::Query::new();
        let mut errors = ParseErrors::new();

        while self.expect_curr(&Token::EOF).is_err() {
            match self.parse_field() {
                Ok(field) => query.fields.push(field),
                Err(err) => errors.push(err),
            }

            dbg!(&self.curr_token);
            let _ = self.expect_curr(&Token::Comma).is_ok();
        }

        if !errors.is_empty() {
            return Err(errors);
        }

        Ok(query)
    }

    pub fn parse_field(&mut self) -> ParseResult<ast::Field> {
        dbg!(&self.curr_token);
        match &self.curr_token.clone() {
            Token::Ident(name) => match &self.peek_token.clone() {
                Token::Lparen => self.parse_expression_field(),
                Token::Comma => {
                    self.next_token();
                    Ok(ast::Field::Table(Box::new(ast::TableField { name: name.to_string() })))
                }
                _ => {
                    self.next_token();
                    Ok(ast::Field::Table(Box::new(ast::TableField { name: name.to_string() })))
                }
            },
            _ => Err(ParseError::UnexpecedToken(self.curr_token.clone())),
        }
    }

    pub fn parse_func_call_expression(&mut self) -> ParseResult<ast::FuncCallExpression> {
        let func_name = self.parse_identifier()?;
        dbg!(&func_name);
        let mut arguments: Vec<ast::Expression> = Vec::new();

        // eat up the left paren
        self.expect_curr(&Token::Lparen)?;
        while self.expect_curr(&Token::Rparen).is_err() {
            let expression = self.parse_expression()?;
            dbg!(&expression);
            arguments.push(expression);

            let _ = self.expect_curr(&Token::Comma).is_ok();
        }

        dbg!(&arguments);
        let func_call_expression = ast::FuncCallExpression {
            func_name: func_name,
            arguments: arguments,
        };

        Ok(func_call_expression)
    }

    pub fn parse_expression(&mut self) -> ParseResult<ast::Expression> {
        if let Token::Ident(ref name) = self.curr_token.clone() {
            self.next_token();
            return Ok(ast::Expression::Identifier(name.to_string()));
        } else if let Token::Int(int) = self.curr_token.clone() {
            self.next_token();
            return Ok(ast::Expression::Int(int));
        }

        Err(ParseError::UnexpecedToken(self.curr_token.clone()))
    }

    pub fn parse_expression_field(&mut self) -> ParseResult<ast::Field> {
        let func_call_expression = self.parse_func_call_expression()?;

        // eat the right paren
        //self.expect_curr(&Token::Rparen)?;

        dbg!(&self.curr_token);
        let group_clause = if self.expect_curr(&Token::Within).is_ok() {
            self.expect_curr(&Token::Group)?;
            Some(self.parse_ordering_clause()?)
        } else {
            None
        };

        dbg!(&group_clause);

        let partition_clause = if self.expect_curr(&Token::Over).is_ok() {
            Some(self.parse_partition_clause()?)
        } else {
            None
        };

        dbg!(&partition_clause);

        let field = ast::ExpressionField {
            func_call_expression: func_call_expression,
            partition_clause: partition_clause,
        };
        Ok(ast::Field::Expression(Box::new(field)))
    }

    fn parse_identifier(&mut self) -> ParseResult<String> {
        if let Token::Ident(ref name) = self.curr_token.clone() {
            self.next_token();
            return Ok(name.to_string());
        }

        Err(ParseError::UnexpecedToken(self.curr_token.clone()))
    }

    pub fn parse_ordering_clause(&mut self) -> ParseResult<ast::OrderingClause> {
        self.expect_curr(&Token::Lparen)?;
        self.expect_curr(&Token::Order)?;
        self.expect_curr(&Token::By)?;

        let name = self.parse_identifier()?;
        dbg!(&name);

        let ordering = if self.expect_curr(&Token::Asc).is_ok() {
            ast::Ordering::Asc
        } else if self.expect_curr(&Token::Desc).is_ok() {
            ast::Ordering::Desc
        } else {
            ast::Ordering::Asc
        };

        self.expect_curr(&Token::Rparen)?;

        let clause = ast::OrderingClause {
            field_name: name,
            ordering: ordering,
        };

        Ok(clause)
    }

    pub fn parse_partition_clause(&mut self) -> ParseResult<ast::PartitionClause> {
        self.expect_curr(&Token::Lparen)?;
        self.expect_curr(&Token::Partition)?;
        self.expect_curr(&Token::By)?;

        let name = self.parse_identifier()?;
        let clause = ast::PartitionClause { field_name: name };
        self.expect_curr(&Token::Rparen)?;
        Ok(clause)
    }

    fn expect_curr(&mut self, token: &Token) -> Result<(), ParseError> {
        match self.curr_token_is(&token) {
            true => {
                self.next_token();
                Ok(())
            }
            false => Err(ParseError::UnexpecedToken(self.curr_token.clone())),
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
    use super::*;
    use crate::lexer::Lexer;

    fn setup(input: &str) -> ast::Query {
        let l = Lexer::new(input);
        let mut p = Parser::new(l);
        let query = p.parse_query().unwrap();

        query
    }

    fn unwrap_expression_field(query: &ast::Query) -> &ast::ExpressionField {
        match query.fields.first().unwrap() {
            ast::Field::Expression(field) => field,
            field => panic!("{:?} isn't an expression field", field),
        }
    }

    fn unwrap_table_field(query: &ast::Query) -> &ast::TableField {
        match query.fields.first().unwrap() {
            ast::Field::Table(field) => field,
            field => panic!("{:?} isn't an expression field", field),
        }
    }

    #[test]
    fn test_table_field() {
        let input = "timestamp, backend_and_port";
        let query = setup(input);
        let field = unwrap_table_field(&query);

        assert_eq!(field.name, "timestamp");
    }

    #[test]
    fn test_aggregate_function_with_partition_clause() {
        let input = "avg(backend_processing_time) over (partition by backend_and_port)";
        let query = setup(input);
        let field = unwrap_expression_field(&query);

        assert_eq!(field.func_call_expression.func_name, "avg");
        assert_eq!(field.func_call_expression.arguments.len(), 1);
    }

    #[test]
    fn test_aggregate_function_with_group_clause() {
        let input = "percentile_disc(1) within group (order by backend_processing_time desc) over (partition by backend_and_port)";
        let query = setup(input);
        let field = unwrap_expression_field(&query);

        assert_eq!(field.func_call_expression.func_name, "percentile_disc");
        assert_eq!(field.func_call_expression.arguments.len(), 1);
    }
}
