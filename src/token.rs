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

    Within,
    Group,

    Order,

    Asc,
    Desc
}

pub fn lookup_ident(ident: String) -> Token {
    match ident.as_str() {
        "over" => Token::Over,
        "partition" => Token::Partition,
        "by" => Token::By,
        "within" => Token::Within,
        "group" => Token::Group,
        "order" => Token::Order,
        "asc" => Token::Asc,
        "desc" => Token::Desc,
        _ => Token::Ident(ident),
    }
}
