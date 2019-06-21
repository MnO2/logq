
#[derive(Debug)]
pub enum Node {
    Field(Box<Field>)
}

#[derive(Debug)]
pub struct Field {
    pub name: String
}