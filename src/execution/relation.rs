use crate::execution::types::{Expression, ExpressionResult, Value, Variables};

pub(crate) trait Relation {
    fn apply(
        &self,
        variables: Variables,
        left: &Box<dyn Expression>,
        right: &Box<dyn Expression>,
    ) -> ExpressionResult<bool>;
}

pub(crate) struct Equal {}

impl Equal {
    pub(crate) fn new() -> Self {
        Equal {}
    }
}

impl Relation for Equal {
    fn apply(
        &self,
        variables: Variables,
        left: &Box<dyn Expression>,
        right: &Box<dyn Expression>,
    ) -> ExpressionResult<bool> {
        let left_result = left.expression_value(variables.clone())?;
        let right_result = right.expression_value(variables.clone())?;

        Ok(left_result == right_result)
    }
}
