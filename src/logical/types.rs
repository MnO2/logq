use crate::common;
use crate::common::types::VariableName;
use crate::execution;
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
        physicalCreator: PhysicalPlanCreator,
    ) -> PhysicalResult<(Box<dyn execution::types::Node>, common::types::Variables)>;
}

pub(crate) trait Expression {
    fn physical(
        &self,
        physicalCreator: PhysicalPlanCreator,
    ) -> PhysicalResult<(Box<dyn execution::types::Expression>, common::types::Variables)>;
}

pub(crate) struct Variable {
    name: VariableName,
}

impl Variable {
    pub(crate) fn new(name: String) -> Self {
        Variable { name }
    }
}

pub(crate) struct DataSource {
    name: String,
}

impl Node for DataSource {
    fn physical(
        &self,
        physicalCreator: PhysicalPlanCreator,
    ) -> PhysicalResult<(Box<dyn execution::types::Node>, common::types::Variables)> {
        let node = Box::new(physicalCreator.data_source);
        let variables = common::types::empty_variables();

        Ok((node, variables))
    }
}

pub(crate) struct PhysicalPlanCreator {
    data_source: execution::types::DataSource,
}
