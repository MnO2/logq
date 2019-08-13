use ordered_float::OrderedFloat;
use std::fmt;
use std::result;
use std::str::FromStr;

#[derive(Debug, PartialEq, Eq, Clone)]
pub(crate) struct SelectStatement {
    pub(crate) select_exprs: Vec<SelectExpression>,
    pub(crate) table_name: String,
    pub(crate) where_expr_opt: Option<WhereExpression>,
    pub(crate) group_by_exprs_opt: Option<GroupByExpression>,
    pub(crate) order_by_expr_opt: Option<OrderByExpression>,
    pub(crate) limit_expr_opt: Option<LimitExpression>,
}

impl SelectStatement {
    pub fn new(
        select_exprs: Vec<SelectExpression>,
        table_name: &str,
        where_expr_opt: Option<WhereExpression>,
        group_by_exprs_opt: Option<GroupByExpression>,
        order_by_expr_opt: Option<OrderByExpression>,
        limit_expr_opt: Option<LimitExpression>,
    ) -> Self {
        SelectStatement {
            select_exprs,
            table_name: table_name.to_string(),
            where_expr_opt,
            group_by_exprs_opt,
            order_by_expr_opt,
            limit_expr_opt,
        }
    }
}

impl fmt::Display for SelectStatement {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let select_exprs_str: Vec<String> = (&self.select_exprs)
            .iter()
            .map(|field| match field {
                SelectExpression::Star => "<star>".to_string(),
                SelectExpression::Expression(e, name_opt) => format!("{:?} as {:?}", e, name_opt),
            })
            .collect();
        write!(f, "{:?}", select_exprs_str)
    }
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub(crate) enum SelectExpression {
    Star,
    Expression(Box<Expression>, Option<String>),
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub(crate) enum Expression {
    Column(ColumnName),
    Value(Value),
    BinaryOperator(BinaryOperator, Box<Expression>, Box<Expression>),
    UnaryOperator(UnaryOperator, Box<Expression>),
    FuncCall(FuncName, Vec<SelectExpression>, Option<WithinGroupClause>),
}

pub(crate) type FuncName = String;
pub(crate) type ColumnName = String;

#[derive(Debug, PartialEq, Eq, Clone)]
pub(crate) enum BinaryOperator {
    Plus,
    Minus,
    Times,
    Divide,
    Equal,
    NotEqual,
    MoreThan,
    LessThan,
    GreaterEqual,
    LessEqual,
    And,
    Or,
}

impl FromStr for BinaryOperator {
    type Err = String;

    fn from_str(s: &str) -> result::Result<Self, Self::Err> {
        match s {
            "+" => Ok(BinaryOperator::Plus),
            "-" => Ok(BinaryOperator::Minus),
            "*" => Ok(BinaryOperator::Times),
            "/" => Ok(BinaryOperator::Divide),
            "=" => Ok(BinaryOperator::Equal),
            "!=" => Ok(BinaryOperator::NotEqual),
            ">" => Ok(BinaryOperator::MoreThan),
            "<" => Ok(BinaryOperator::LessThan),
            ">=" => Ok(BinaryOperator::GreaterEqual),
            "<=" => Ok(BinaryOperator::LessEqual),
            "and" => Ok(BinaryOperator::And),
            "or" => Ok(BinaryOperator::Or),
            _ => Err("unknown binary operator".to_string()),
        }
    }
}



impl fmt::Display for BinaryOperator {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub(crate) enum UnaryOperator {
    Not,
}

impl fmt::Display for UnaryOperator {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:?}", self)
    }
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
    pub(crate) exprs: Vec<ColumnName>,
}

impl GroupByExpression {
    pub(crate) fn new(exprs: Vec<ColumnName>) -> Self {
        GroupByExpression { exprs }
    }
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub(crate) struct LimitExpression {
    pub(crate) row_count: u32,
}

impl LimitExpression {
    pub(crate) fn new(row_count: u32) -> Self {
        LimitExpression { row_count }
    }
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub(crate) enum Ordering {
    Asc,
    Desc,
}

impl FromStr for Ordering {
    type Err = String;

    fn from_str(s: &str) -> result::Result<Self, Self::Err> {
        match s {
            "asc" => Ok(Ordering::Asc),
            "desc" => Ok(Ordering::Desc),
            _ => Err("unknown ordering".to_string()),
        }
    }
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub(crate) struct OrderingTerm {
    pub(crate) column_name: String,
    pub(crate) ordering: Ordering,
}

impl OrderingTerm {
    pub(crate) fn new(column_name: &str, ordering: &str) -> Self {
        OrderingTerm {
            column_name: column_name.to_string(),
            ordering: Ordering::from_str(ordering).unwrap(),
        }
    }
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub(crate) struct OrderByExpression {
    pub(crate) ordering_terms: Vec<OrderingTerm>,
}

impl OrderByExpression {
    pub(crate) fn new(ordering_terms: Vec<OrderingTerm>) -> Self {
        OrderByExpression { ordering_terms }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct FuncCallExpression {
    pub(crate) func_name: String,
    pub(crate) arguments: Vec<Expression>,
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub(crate) struct WithinGroupClause {
    pub(crate) ordering_term: OrderingTerm,
}

impl WithinGroupClause {
    pub(crate) fn new(order_by_expr: OrderByExpression) -> Self {
        WithinGroupClause {
            ordering_term: order_by_expr.ordering_terms[0].clone(),
        }
    }
}
