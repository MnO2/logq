use crate::common::types as common;
use crate::common::types::VariableName;
use crate::execution::logic;
use crate::execution::types as execution;
use std::path::PathBuf;
use std::result;

pub(crate) type PhysicalResult<T> = result::Result<T, PhysicalError>;

#[derive(Fail, PartialEq, Eq, Debug)]
pub enum PhysicalError {
    #[fail(display = "Key Not Found")]
    KeyNotFound,
}

pub(crate) trait Node {
    fn physical(
        &self,
        physicalCreator: &mut PhysicalPlanCreator,
    ) -> PhysicalResult<(Box<dyn execution::Node>, common::Variables)>;
}

pub(crate) trait Expression {
    fn physical(
        &self,
        physicalCreator: &mut PhysicalPlanCreator,
    ) -> PhysicalResult<(Box<dyn execution::Expression>, common::Variables)>;
}

pub(crate) trait Formula {
    fn physical(
        &self,
        physicalCreator: &mut PhysicalPlanCreator,
    ) -> PhysicalResult<(Box<dyn execution::Formula>, common::Variables)>;
}

pub(crate) struct Variable {
    name: VariableName,
}

impl Expression for Variable {
    fn physical(
        &self,
        physicalCreator: &mut PhysicalPlanCreator,
    ) -> PhysicalResult<(Box<dyn execution::Expression>, common::Variables)> {
        let node = Box::new(execution::Variable::new("a".to_string()));
        let variables = common::empty_variables();

        Ok((node, variables))
    }
}

impl Variable {
    pub(crate) fn new(name: String) -> Self {
        Variable { name }
    }
}

pub(crate) struct LogicExpression {
    pub(crate) formula: Box<dyn Formula>,
}

impl LogicExpression {
    pub(crate) fn new(formula: Box<dyn Formula>) -> Self {
        LogicExpression { formula }
    }
}

impl Expression for LogicExpression {
    fn physical(
        &self,
        physicalCreator: &mut PhysicalPlanCreator,
    ) -> PhysicalResult<(Box<dyn execution::Expression>, common::Variables)> {
        let (expr, variables) = self.formula.physical(physicalCreator)?;

        Ok((Box::new(execution::LogicExpression::new(expr)), variables))
    }
}

pub(crate) struct PrefixOperator {
    pub(crate) op: String,
    pub(crate) child: Box<dyn Formula>,
}

impl PrefixOperator {
    pub(crate) fn new(op: String, child: Box<dyn Formula>) -> Self {
        PrefixOperator { op, child }
    }
}

impl Formula for PrefixOperator {
    fn physical(
        &self,
        physicalCreator: &mut PhysicalPlanCreator,
    ) -> PhysicalResult<(Box<dyn execution::Formula>, common::Variables)> {
        let (child, child_variables) = self.child.physical(physicalCreator)?;

        Ok((Box::new(logic::Not::new(child)), child_variables))
    }
}

pub(crate) struct InfixOperator {
    pub(crate) op: String,
    pub(crate) left: Box<dyn Formula>,
    pub(crate) right: Box<dyn Formula>,
}

impl InfixOperator {
    pub(crate) fn new(op: String, left: Box<dyn Formula>, right: Box<dyn Formula>) -> Self {
        InfixOperator { op, left, right }
    }
}

impl Formula for InfixOperator {
    fn physical(
        &self,
        physicalCreator: &mut PhysicalPlanCreator,
    ) -> PhysicalResult<(Box<dyn execution::Formula>, common::Variables)> {
        let (left, left_variables) = self.left.physical(physicalCreator)?;
        let (right, right_variables) = self.right.physical(physicalCreator)?;

        let return_variables = common::merge(left_variables, right_variables);

        Ok((Box::new(logic::And::new(left, right)), return_variables))
    }
}

pub(crate) enum DataSource {
    File(PathBuf),
    Stdin,
}

impl Node for DataSource {
    fn physical(
        &self,
        physicalCreator: &mut PhysicalPlanCreator,
    ) -> PhysicalResult<(Box<dyn execution::Node>, common::Variables)> {
        let node = Box::new(physicalCreator.data_source.clone());
        let variables = common::empty_variables();

        Ok((node, variables))
    }
}

pub(crate) struct PhysicalPlanCreator {
    data_source: execution::DataSource,
}
