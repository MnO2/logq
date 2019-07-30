use std::fmt;

#[derive(Debug)]
pub enum Node {
    Query(Box<Query>),
}

#[derive(Debug)]
pub enum Field {
    Table(Box<TableField>),
    Expression(Box<ExpressionField>),
}

#[derive(Debug)]
pub struct Query {
    pub fields: Vec<Field>,
}

impl Query {
    pub fn new() -> Self {
        Query { fields: Vec::new() }
    }
}

impl fmt::Display for Query {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        //let fields: Vec<String> = (&self.fields).into_iter().map(|field| field.name.clone()).collect();
        write!(f, "{}", "")
    }
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub enum Expression {
    Identifier(String),
    Int(i64),
}

impl Expression {
    pub fn ident(&self) -> String {
        match self {
            Expression::Identifier(ref s) => s.to_string(),
            _ => panic!("Expression is not an Identifier"),
        }
    }
}

#[derive(Debug)]
pub struct TableField {
    pub name: String,
}

#[derive(Debug, Clone)]
pub struct ExpressionField {
    pub func_call_expression: FuncCallExpression,
    pub partition_clause: Option<PartitionClause>,
}

#[derive(Debug, Clone)]
pub struct PartitionClause {
    pub field_name: String,
}

#[derive(Debug)]
pub struct OrderingClause {
    pub field_name: String,
    pub ordering: Ordering,
}

#[derive(Debug)]
pub enum Ordering {
    Asc,
    Desc,
}

#[derive(Debug, Clone)]
pub struct FuncCallExpression {
    pub func_name: String,
    pub arguments: Vec<Expression>,
}
