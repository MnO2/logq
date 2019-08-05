use crate::common::types as common;
use crate::common::types::VariableName;
use crate::execution::types as execution;
use std::path::PathBuf;
use std::result;

pub(crate) type PhysicalResult<T> = result::Result<T, PhysicalPlanError>;

#[derive(Fail, PartialEq, Eq, Debug)]
pub enum PhysicalPlanError {
    #[fail(display = "Type Mismatch")]
    TypeMisMatch,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Node {
    DataSource(DataSource),
    Filter(Box<Formula>, Box<Node>),
}

impl Node {
    pub(crate) fn physical(
        &self,
        physicalCreator: &mut PhysicalPlanCreator,
    ) -> PhysicalResult<(Box<execution::Node>, common::Variables)> {
        match self {
            Node::DataSource(data_source) => {
                let node = physicalCreator.data_source.clone();
                let variables = common::empty_variables();

                Ok((Box::new(node), variables))
            }
            Node::Filter(formula, source) => {
                let (physical_formula, formula_variables) = formula.physical(physicalCreator)?;
                let (child, child_variables) = source.physical(physicalCreator)?;

                let return_variables = common::merge(formula_variables, child_variables);
                let filter = execution::Node::Filter(child, physical_formula);
                Ok((Box::new(filter), return_variables))
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Expression {
    Variable(VariableName),
    LogicExpression(Box<Formula>),
    FunctionExpression(String, Vec<Box<Expression>>),
}

impl Expression {
    pub(crate) fn physical(
        &self,
        physicalCreator: &mut PhysicalPlanCreator,
    ) -> PhysicalResult<(Box<execution::Expression>, common::Variables)> {
        match self {
            Expression::Variable(name) => {
                let node = Box::new(execution::Expression::Variable("a".to_string()));
                let variables = common::empty_variables();

                Ok((node, variables))
            }
            Expression::LogicExpression(formula) => {
                let (expr, variables) = formula.physical(physicalCreator)?;
                Ok((Box::new(execution::Expression::LogicExpression(expr)), variables))
            }
            Expression::FunctionExpression(name, arguments) => {
                unimplemented!();
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Formula {
    InfixOperator(String, Box<Formula>, Box<Formula>),
    PrefixOperator(String, Box<Formula>),
    Constant(common::Value),
    Predicate(Box<Relation>, Box<Expression>, Box<Expression>),
}

impl Formula {
    pub(crate) fn physical(
        &self,
        physicalCreator: &mut PhysicalPlanCreator,
    ) -> PhysicalResult<(Box<execution::Formula>, common::Variables)> {
        match self {
            Formula::InfixOperator(op, left_formula, right_formula) => {
                let (left, left_variables) = left_formula.physical(physicalCreator)?;
                let (right, right_variables) = right_formula.physical(physicalCreator)?;

                let return_variables = common::merge(left_variables, right_variables);

                Ok((Box::new(execution::Formula::And(left, right)), return_variables))
            }
            Formula::PrefixOperator(op, child_formula) => {
                let (child, child_variables) = child_formula.physical(physicalCreator)?;
                Ok((Box::new(execution::Formula::Not(child)), child_variables))
            }
            Formula::Constant(value) => match value {
                common::Value::Boolean(b) => {
                    let node = Box::new(execution::Formula::Constant(*b));
                    let variables = common::empty_variables();

                    Ok((node, variables))
                }
                _ => Err(PhysicalPlanError::TypeMisMatch),
            },
            Formula::Predicate(relation, left_expr, right_expr) => {
                let (left, left_variables) = left_expr.physical(physicalCreator)?;
                let (right, right_variables) = right_expr.physical(physicalCreator)?;

                let physical_relation = relation.physical(physicalCreator)?;

                let return_variables = common::merge(left_variables, right_variables);
                Ok((
                    Box::new(execution::Formula::Predicate(physical_relation, left, right)),
                    return_variables,
                ))
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum DataSource {
    File(PathBuf),
    Stdin,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PhysicalPlanCreator {
    data_source: execution::Node,
}

impl PhysicalPlanCreator {
    pub(crate) fn new(name: String) -> Self {
        let data_source = execution::Node::DataSource(name);
        PhysicalPlanCreator { data_source }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum AggregateFunction {
    Avg,
    Count,
    First,
    Last,
    Max,
    Min,
    Sum,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Aggregate {
    aggregate_func: AggregateFunction,
    argument: Option<Box<Expression>>,
}

impl Aggregate {
    pub(crate) fn new(aggregate_func: AggregateFunction, argument: Option<Box<Expression>>) -> Self {
        Aggregate {
            aggregate_func,
            argument,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Relation {
    Equal,
    NotEqual,
    MoreThan,
    LessThan,
    GreaterEqual,
    LessEqual,
}

impl Relation {
    pub(crate) fn physical(
        &self,
        physicalCreator: &mut PhysicalPlanCreator,
    ) -> PhysicalResult<Box<execution::Relation>> {
        match self {
            Relation::Equal => Ok(Box::new(execution::Relation::Equal)),
            Relation::NotEqual => Ok(Box::new(execution::Relation::NotEqual)),
            Relation::MoreThan => Ok(Box::new(execution::Relation::MoreThan)),
            Relation::LessThan => Ok(Box::new(execution::Relation::LessThan)),
            Relation::GreaterEqual => Ok(Box::new(execution::Relation::GreaterEqual)),
            Relation::LessEqual => Ok(Box::new(execution::Relation::LessEqual)),
        }
    }
}
