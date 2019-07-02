use crate::token::{self, Token};
use std::iter::Peekable;
use std::str::Chars;

pub struct Lexer<'a> {
    input: Peekable<Chars<'a>>,
}

impl<'a> Lexer<'a> {
    pub fn new(input: &'a str) -> Lexer {
        Lexer {
            input: input.chars().peekable(),
        }
    }

    pub fn next_token(&mut self) -> Token {
        self.eat_whitespace();

        match self.read_char() {
            Some(',') => Token::Comma,
            Some('(') => Token::Lparen,
            Some(')') => Token::Rparen,
            Some(ch) => {
                if is_letter(ch) {
                    let ident = self.read_identifier(ch);
                    token::lookup_ident(ident)
                } else if ch.is_digit(10) {
                    let i = self.read_int(ch);
                    Token::Int(i)
                } else {
                    Token::Illegal
                }
            }
            None => Token::EOF,
        }
    }

    fn eat_whitespace(&mut self) {
        while let Some(&ch) = self.input.peek() {
            if ch.is_whitespace() {
                self.read_char();
            } else {
                break;
            }
        }
    }

    fn peek_char(&mut self) -> Option<&char> {
        self.input.peek()
    }

    fn read_char(&mut self) -> Option<char> {
        self.input.next()
    }

    fn read_int(&mut self, ch: char) -> i64 {
        let mut s = String::new();
        s.push(ch);

        while let Some(&ch) = self.peek_char() {
            if ch.is_digit(10) {
                s.push(self.read_char().unwrap());
            } else {
                break;
            }
        }

        s.parse().unwrap()
    }

    fn read_identifier(&mut self, ch: char) -> String {
        let mut ident = String::new();
        ident.push(ch);

        while let Some(&ch) = self.peek_char() {
            if is_letter(ch) {
                ident.push(self.read_char().unwrap());
            } else {
                break;
            }
        }

        ident
    }
}

fn is_letter(ch: char) -> bool {
    ch.is_alphabetic() || ch == '_'
}

#[cfg(test)]
mod test {
    use super::*;
    use token::Token;

    #[test]
    fn test_next_token() {
        let input = r#"timestamp, backend_and_port"#;

        let ans = vec![
            Token::Ident("timestamp".to_string()),
            Token::Comma,
            Token::Ident("backend_and_port".to_string()),
        ];

        let mut l = Lexer::new(input);

        for t in ans.iter() {
            let tok = l.next_token();

            assert_eq!(*t, tok, "expected {:?} token but got {:?}", t, tok);
        }
    }
}
