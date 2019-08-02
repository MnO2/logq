use ordered_float::OrderedFloat;
use std::fmt;

#[derive(Debug, PartialEq, Eq, Clone)]
pub(crate) struct SelectStatement {
    pub(crate) select_exprs: Vec<SelectExpression>,
    pub(crate) where_expr_opt: Option<WhereExpression>,
    pub(crate) group_by_expr_opt: Option<GroupByExpression>,
}

impl SelectStatement {
    pub fn new(
        select_exprs: Vec<SelectExpression>,
        where_expr_opt: Option<WhereExpression>,
        group_by_expr_opt: Option<GroupByExpression>,
    ) -> Self {
        SelectStatement {
            select_exprs,
            where_expr_opt,
            group_by_expr_opt,
        }
    }
}

impl fmt::Display for SelectStatement {
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
    ComparisonExpression(RelationOperator, Box<ValueExpression>, Box<ValueExpression>),
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub(crate) enum RelationOperator {
    Equal,
    NotEqual,
}

pub(crate) type FuncName = String;
pub(crate) type ColumnName = String;

#[derive(Debug, PartialEq, Eq, Clone)]
pub(crate) enum ValueExpression {
    Column(ColumnName),
    Value(Value),
    Operator(ValueOperator, Box<ValueExpression>, Box<ValueExpression>),
    FuncCall(FuncName, Vec<Box<SelectExpression>>),
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub(crate) enum ValueOperator {
    Plus,
    Minus,
    Times,
    Divide,
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub enum Value {
    Integral(i32),
    Float(OrderedFloat<f32>),
    StringLiteral(String),
    Boolean(bool),
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub(crate) struct WhereExpression {
    pub(crate) expr: Expression,
}

impl WhereExpression {
    pub(crate) fn new(expr: Expression) -> Self {
        WhereExpression { expr }
    }
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub(crate) struct GroupByExpression {
    pub(crate) expr: Expression,
}

impl GroupByExpression {
    pub(crate) fn new(expr: Expression) -> Self {
        GroupByExpression { expr }
    }
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
