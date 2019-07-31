use crate::execution::types::{Formula, EvaluateResult, Variables};

struct Constant {
    value: bool,
}

impl Constant {
    fn new(value: bool) -> Self {
        Constant { value }
    }
}

struct And {
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

struct Or {
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

