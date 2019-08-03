use crate::common::types as common;
use crate::common::types::VariableName;
use crate::execution::types as execution;
use std::path::PathBuf;
use std::result;

pub(crate) type PhysicalResult<T> = result::Result<T, PhysicalError>;

#[derive(Fail, PartialEq, Eq, Debug)]
pub enum PhysicalError {
    #[fail(display = "Key Not Found")]
    KeyNotFound,
    #[fail(display = "Type Mismatch")]
    TypeMisMatch,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Node {
    DataSource(DataSource),
    Filter(Box<Formula>, Box<Node>),
}

impl Node {
    fn physical(
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
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Formula {
    InfixOperator(String, Box<Formula>, Box<Formula>),
    PrefixOperator(String, Box<Formula>),
    Constant(common::Value),
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
                _ => Err(PhysicalError::TypeMisMatch),
            },
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Aggregate {
    Avg,
    Count,
    First,
    Last,
    Max,
    Min,
    Sum,
}
