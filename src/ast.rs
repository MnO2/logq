use std::fmt;

#[derive(Debug)]
pub enum Node {
    Query(Box<Query>)
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
        let fields: Vec<String> = (&self.fields).into_iter().map(|field| field.name.clone()).collect();
        write!(f, "{}", fields.join(""))
    }
}

#[derive(Debug)]
pub struct Field {
    pub name: String
}