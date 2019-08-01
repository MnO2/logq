use crate::common::types::Variables;
use crate::execution::relation::Relation;
use crate::execution::types::{EvaluateResult, Expression, Formula};

pub(crate) struct Constant {
    value: bool,
}

impl Constant {
    fn new(value: bool) -> Self {
        Constant { value }
    }
}

pub(crate) struct And {
    left: Box<dyn Formula>,
    right: Box<dyn Formula>,
}

impl And {
    fn new(left: Box<dyn Formula>, right: Box<dyn Formula>) -> Self {
        And { left, right }
    }
}

impl Formula for And {
    fn evaluate(&self, variables: Variables) -> EvaluateResult<bool> {
        let left = self.left.evaluate(variables.clone())?;
        let right = self.right.evaluate(variables.clone())?;
        Ok(left && right)
    }
}

pub(crate) struct Or {
    left: Box<dyn Formula>,
    right: Box<dyn Formula>,
}

impl Or {
    fn new(left: Box<dyn Formula>, right: Box<dyn Formula>) -> Self {
        Or { left, right }
    }
}

impl Formula for Or {
    fn evaluate(&self, variables: Variables) -> EvaluateResult<bool> {
        let left = self.left.evaluate(variables.clone())?;
        let right = self.right.evaluate(variables.clone())?;
        Ok(left || right)
    }
}

pub(crate) struct Predicate {
    left: Box<dyn Expression>,
    right: Box<dyn Expression>,
    relation: Box<dyn Relation>,
}

impl Predicate {
    pub(crate) fn new(left: Box<dyn Expression>, right: Box<dyn Expression>, relation: Box<dyn Relation>) -> Self {
        Predicate { left, right, relation }
    }
}

impl Formula for Predicate {
    fn evaluate(&self, variables: Variables) -> EvaluateResult<bool> {
        let result = self.relation.apply(variables, &self.left, &self.right)?;
        Ok(result)
    }
}
