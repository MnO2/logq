use crate::common::types::VariableName;
use ordered_float::OrderedFloat;
use std::fmt;
use std::result;
use std::str::FromStr;

#[derive(Debug, PartialEq, Eq, Clone)]
pub(crate) enum SetOperator {
    Union,
    Intersect,
    Except,
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub(crate) enum Query {
    Select(SelectStatement),
    SetOp {
        op: SetOperator,
        all: bool,
        left: Box<Query>,
        right: Box<Query>,
    },
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub(crate) enum JoinType {
    Cross,
    Left,
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub(crate) enum FromClause {
    /// Single table or comma-separated tables (existing behavior)
    Tables(Vec<TableReference>),
    /// JOIN: left source, right table, join type, ON condition
    Join {
        left: Box<FromClause>,
        right: TableReference,
        join_type: JoinType,
        condition: Option<Expression>, // None for CROSS JOIN, Some for LEFT JOIN
    },
}

impl FromClause {
    /// Collect all table references from the FromClause tree (for env checking).
    pub(crate) fn collect_table_references(&self) -> Vec<&TableReference> {
        match self {
            FromClause::Tables(refs) => refs.iter().collect(),
            FromClause::Join { left, right, .. } => {
                let mut refs = left.collect_table_references();
                refs.push(right);
                refs
            }
        }
    }
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub(crate) struct SelectStatement {
    pub(crate) distinct: bool,
    pub(crate) select_clause: SelectClause,
    pub(crate) from_clause: FromClause,
    pub(crate) where_expr_opt: Option<WhereExpression>,
    pub(crate) group_by_exprs_opt: Option<GroupByExpression>,
    pub(crate) having_expr_opt: Option<WhereExpression>,
    pub(crate) order_by_expr_opt: Option<OrderByExpression>,
    pub(crate) limit_expr_opt: Option<LimitExpression>,
}

impl SelectStatement {
    pub fn new(
        distinct: bool,
        select_clause: SelectClause,
        from_clause: FromClause,
        where_expr_opt: Option<WhereExpression>,
        group_by_exprs_opt: Option<GroupByExpression>,
        having_expr_opt: Option<WhereExpression>,
        order_by_expr_opt: Option<OrderByExpression>,
        limit_expr_opt: Option<LimitExpression>,
    ) -> Self {
        SelectStatement {
            distinct,
            select_clause,
            from_clause,
            where_expr_opt,
            group_by_exprs_opt,
            having_expr_opt,
            order_by_expr_opt,
            limit_expr_opt,
        }
    }
}

impl fmt::Display for SelectStatement {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match &self.select_clause {
            SelectClause::SelectExpressions(select_exprs) => {
                let select_exprs_str: Vec<String> = select_exprs
                    .iter()
                    .map(|field| match field {
                        SelectExpression::Star => "<star>".to_string(),
                        SelectExpression::Expression(e, name_opt) => format!("{:?} as {:?}", e, name_opt),
                    })
                    .collect();
                write!(f, "{:?}", select_exprs_str)
            }
            SelectClause::ValueConstructor(vc) => {
                write!(f, "SELECT VALUE {:?}", vc)
            }
        }
    }
}

#[derive(Debug, PartialEq, Eq, Clone, Hash, PartialOrd, Ord)]
pub(crate) enum PathSegment {
    AttrName(String),
    ArrayIndex(String, usize),
    Wildcard,     // [*] — iterate all array elements
    WildcardAttr, // .* — iterate all tuple values
}

#[derive(Debug, PartialEq, Eq, Clone, Hash, PartialOrd, Ord)]
pub(crate) struct PathExpr {
    pub(crate) path_segments: Vec<PathSegment>,
}

impl PathExpr {
    pub fn new(path_segments: Vec<PathSegment>) -> Self {
        PathExpr { path_segments }
    }

    pub fn unwrap_last(&self) -> VariableName {
        match &self.path_segments.last().unwrap() {
            PathSegment::AttrName(s) => s.clone(),
            PathSegment::ArrayIndex(s, _idx) => s.clone(),
            PathSegment::Wildcard => "[*]".to_string(),
            PathSegment::WildcardAttr => ".*".to_string(),
        }
    }
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub(crate) struct TableReference {
    pub(crate) path_expr: PathExpr,
    pub(crate) as_clause: Option<String>,
    pub(crate) at_clause: Option<String>,
}

impl TableReference {
    pub fn new(path_expr: PathExpr, as_clause: Option<String>, at_clause: Option<String>) -> Self {
        TableReference {
            path_expr,
            as_clause,
            at_clause,
        }
    }
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub(crate) struct TupleConstructor {
    pub(crate) key_values: Vec<(String, Expression)>,
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub(crate) struct ArrayConstructor {
    pub(crate) values: Vec<Expression>,
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub(crate) enum SelectClause {
    ValueConstructor(ValueConstructor),
    SelectExpressions(Vec<SelectExpression>),
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub(crate) enum ValueConstructor {
    Expression(Expression),
    TupleConstructor(TupleConstructor),
    ArrayConstructor(ArrayConstructor),
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub(crate) enum SelectExpression {
    Star,
    Expression(Box<Expression>, Option<String>),
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub(crate) struct CaseWhenExpression {
    pub(crate) branches: Vec<(Expression, Expression)>,
    pub(crate) else_expr: Option<Box<Expression>>,
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub(crate) enum Expression {
    Column(PathExpr),
    Value(Value),
    BinaryOperator(BinaryOperator, Box<Expression>, Box<Expression>),
    UnaryOperator(UnaryOperator, Box<Expression>),
    FuncCall(FuncName, Vec<SelectExpression>, Option<WithinGroupClause>),
    CaseWhenExpression(CaseWhenExpression),
    IsNull(Box<Expression>),
    IsNotNull(Box<Expression>),
    IsMissing(Box<Expression>),
    IsNotMissing(Box<Expression>),
    Like(Box<Expression>, Box<Expression>),
    NotLike(Box<Expression>, Box<Expression>),
    In(Box<Expression>, Vec<Expression>),
    NotIn(Box<Expression>, Vec<Expression>),
    Between(Box<Expression>, Box<Expression>, Box<Expression>),      // expr BETWEEN lo AND hi
    NotBetween(Box<Expression>, Box<Expression>, Box<Expression>),   // expr NOT BETWEEN lo AND hi
    Cast(Box<Expression>, CastType),
    Subquery(Box<SelectStatement>),  // A parenthesized SELECT statement used as a scalar expression
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub(crate) enum CastType {
    Int,
    Float,
    Varchar,
    Boolean,
}

pub(crate) type FuncName = String;

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
    Concat,
}

impl FromStr for BinaryOperator {
    type Err = String;

    fn from_str(s: &str) -> result::Result<Self, Self::Err> {
        let lower = s.to_ascii_lowercase();
        match lower.as_str() {
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
            "||" => Ok(BinaryOperator::Concat),
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
    Null,
    Missing,
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
pub(crate) struct GroupByReference {
    pub(crate) column_expr: Expression,
    pub(crate) as_clause: Option<String>,
}

impl GroupByReference {
    pub fn new(column_expr: Expression, as_clause: Option<String>) -> Self {
        GroupByReference { column_expr, as_clause }
    }
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub(crate) struct GroupByExpression {
    pub(crate) exprs: Vec<GroupByReference>,
    pub(crate) group_as_clause: Option<String>,
}

impl GroupByExpression {
    pub(crate) fn new(exprs: Vec<GroupByReference>, group_as_clause: Option<String>) -> Self {
        GroupByExpression { exprs, group_as_clause }
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
        let lower = s.to_ascii_lowercase();
        match lower.as_str() {
            "asc" => Ok(Ordering::Asc),
            "desc" => Ok(Ordering::Desc),
            _ => Err("unknown ordering".to_string()),
        }
    }
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub(crate) struct OrderingTerm {
    pub(crate) column_name: PathExpr,
    pub(crate) ordering: Ordering,
}

impl OrderingTerm {
    pub(crate) fn new(column_name: PathExpr, ordering: &str) -> Self {
        OrderingTerm {
            column_name,
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
