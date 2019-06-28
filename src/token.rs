#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub enum Token {
    EOF,
    Illegal,

    Ident(String),
    Int(i64),

    Comma,

    Lparen,
    Rparen,

    Over,
    Partition,
    By,
}

pub fn lookup_ident(ident: String) -> Token {
    match ident.as_str() {
        "over" => Token::Over,
        "partition" => Token::Partition,
        "by" => Token::By,
        _ => Token::Ident(ident),
    }
}
