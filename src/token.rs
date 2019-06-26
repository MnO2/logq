
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub enum Token {
    EOF,
    Illegal,

    Ident(String),

    Comma,

    Lparen,
    Rparen,
}
