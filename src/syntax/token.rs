#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub enum Token {
    EOF,
    Illegal,

    Ident(String),
    Int(i64),

    Star,
    Comma,

    Lparen,
    Rparen,

    Over,
    By,
    Group,
    Order,

    True,
    False,

    Asc,
    Desc,
}

pub fn lookup_ident(ident: String) -> Token {
    match ident.as_str() {
        "over" => Token::Over,
        "by" => Token::By,
        "group" => Token::Group,
        "order" => Token::Order,
        "asc" => Token::Asc,
        "desc" => Token::Desc,
        _ => Token::Ident(ident),
    }
}
