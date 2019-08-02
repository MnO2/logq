use std::fmt;

#[derive(Debug)]
pub(crate) enum Node {
    Query(Box<Query>),
}

#[derive(Debug)]
pub(crate) struct Query {
    pub select_exprs: Vec<SelectExpression>,
}

impl Query {
    pub fn new() -> Self {
        Query {
            select_exprs: Vec::new(),
        }
    }
}

impl fmt::Display for Query {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let select_exprs_str: Vec<String> = (&self.select_exprs)
            .iter()
            .map(|field| match field {
                SelectExpression::Star => "<star>".to_string(),
                SelectExpression::Expression(e) => format!("{:?}", e),
            })
            .collect();
        write!(f, "{:?}", select_exprs_str)
    }
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub(crate) enum SelectExpression {
    Star,
    Expression(Box<Expression>),
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub(crate) enum Expression {
    Condition(Condition),
    And(Box<Expression>, Box<Expression>),
    Or(Box<Expression>, Box<Expression>),
    Not(Box<Expression>),
    Value(Box<ValueExpression>),
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub(crate) enum Condition {
    ComparisonExpression(OperatorName, Box<ValueExpression>, Box<ValueExpression>),
}

pub(crate) type FuncName = String;
pub(crate) type OperatorName = String;
pub(crate) type ColumnName = String;

#[derive(Debug, PartialEq, Eq, Clone)]
pub(crate) enum ValueExpression {
    Column(ColumnName),
    Int(i64),
    Operator(OperatorName, Box<ValueExpression>, Box<ValueExpression>),
    FuncCall(FuncName, Vec<Box<SelectExpression>>),
}

#[derive(Debug, Clone)]
pub(crate) struct WhereClause {
    pub(crate) expr: Expression,
}

#[derive(Debug, Clone)]
pub(crate) enum BooleanValue {
    True,
    False,
}

#[derive(Debug, Clone)]
pub(crate) enum Ordering {
    Asc,
    Desc,
}

#[derive(Debug, Clone)]
pub(crate) struct FuncCallExpression {
    pub(crate) func_name: String,
    pub(crate) arguments: Vec<ValueExpression>,
}
