use std::fmt;

#[derive(Debug)]
pub enum Node {
    Query(Box<Query>)
}

#[derive(Debug)]
pub enum Field {
    Table(Box<TableField>),
    Expression(Box<ExpressionField>)
}

#[derive(Debug)]
pub struct Query {
    pub fields: Vec<Field>
}

impl Query {
    pub fn new() -> Self {
        Query {
            fields: Vec::new(),
        }
    }
}

impl fmt::Display for Query {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        //let fields: Vec<String> = (&self.fields).into_iter().map(|field| field.name.clone()).collect();
        write!(f, "{}", "")
    }
}

#[derive(Debug)]
pub struct TableField {
    pub name: String
}

#[derive(Debug)]
pub struct ExpressionField {
    pub func_name: String,
    pub arguments: Vec<String>
}