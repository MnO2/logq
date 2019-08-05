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
    Map(Vec<NamedExpression>, Box<Node>),
    GroupBy(Vec<VariableName>, Vec<Aggregate>, Box<Node>),
}

impl Node {
    pub(crate) fn physical(
        &self,
        physical_plan_creator: &mut PhysicalPlanCreator,
    ) -> PhysicalResult<(Box<execution::Node>, common::Variables)> {
        match self {
            Node::DataSource(data_source) => {
                let node = physical_plan_creator.data_source.clone();
                let variables = common::empty_variables();

                Ok((Box::new(node), variables))
            }
            Node::Filter(formula, source) => {
                let (physical_formula, formula_variables) = formula.physical(physical_plan_creator)?;
                let (child, child_variables) = source.physical(physical_plan_creator)?;

                let return_variables = common::merge(formula_variables, child_variables);
                let filter = execution::Node::Filter(child, physical_formula);
                Ok((Box::new(filter), return_variables))
            }
            Node::Map(expressions, source) => {
                let mut physical_expressions: Vec<execution::NamedExpression> = Vec::new();
                let mut total_expression_variables = common::empty_variables();

                for expression in expressions.iter() {
                    let (physical_expression, expression_variables) = expression.physical(physical_plan_creator)?;
                    physical_expressions.push(*physical_expression);
                    total_expression_variables = common::merge(total_expression_variables, expression_variables);
                }

                let (child, child_variables) = source.physical(physical_plan_creator)?;
                let return_variables = common::merge(total_expression_variables, child_variables);

                let node = execution::Node::Map(physical_expressions, child);

                Ok((Box::new(node), return_variables))
            }
            Node::GroupBy(fields, aggergates, source) => {
                let mut variables = common::empty_variables();

                let mut physical_aggregates = Vec::new();
                for aggregate in aggergates.iter() {
                    let (physical_aggregate, aggregate_variables) = aggregate.physical(physical_plan_creator)?;
                    variables = common::merge(variables, aggregate_variables);
                    physical_aggregates.push(physical_aggregate);
                }
                let (child, child_variables) = source.physical(physical_plan_creator)?;
                let return_variables = common::merge(variables, child_variables);

                let node = execution::Node::GroupBy(fields.clone(), physical_aggregates, child);

                Ok((Box::new(node), return_variables))
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct NamedExpression {
    pub(crate) expr: Expression,
    pub(crate) name: VariableName,
}

impl NamedExpression {
    pub(crate) fn new(expr: Expression, name: VariableName) -> Self {
        NamedExpression { expr, name }
    }

    pub(crate) fn physical(
        &self,
        physical_plan_creator: &mut PhysicalPlanCreator,
    ) -> PhysicalResult<(Box<execution::NamedExpression>, common::Variables)> {
        let (physical_expr, expr_variables) = self.expr.physical(physical_plan_creator)?;
        Ok((
            Box::new(execution::NamedExpression {
                expr: physical_expr,
                name: self.name.clone(),
            }),
            expr_variables,
        ))
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
        physical_plan_creator: &mut PhysicalPlanCreator,
    ) -> PhysicalResult<(Box<execution::Expression>, common::Variables)> {
        match self {
            Expression::Variable(name) => {
                let node = Box::new(execution::Expression::Variable("a".to_string()));
                let variables = common::empty_variables();

                Ok((node, variables))
            }
            Expression::LogicExpression(formula) => {
                let (expr, variables) = formula.physical(physical_plan_creator)?;
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
    Predicate(Relation, Box<Expression>, Box<Expression>),
}

impl Formula {
    pub(crate) fn physical(
        &self,
        physical_plan_creator: &mut PhysicalPlanCreator,
    ) -> PhysicalResult<(Box<execution::Formula>, common::Variables)> {
        match self {
            Formula::InfixOperator(op, left_formula, right_formula) => {
                let (left, left_variables) = left_formula.physical(physical_plan_creator)?;
                let (right, right_variables) = right_formula.physical(physical_plan_creator)?;

                let return_variables = common::merge(left_variables, right_variables);

                Ok((Box::new(execution::Formula::And(left, right)), return_variables))
            }
            Formula::PrefixOperator(op, child_formula) => {
                let (child, child_variables) = child_formula.physical(physical_plan_creator)?;
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
                let (left, left_variables) = left_expr.physical(physical_plan_creator)?;
                let (right, right_variables) = right_expr.physical(physical_plan_creator)?;

                let physical_relation = relation.physical(physical_plan_creator)?;

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

impl AggregateFunction {
    pub(crate) fn physical(
        &self,
        physical_plan_creator: &mut PhysicalPlanCreator,
    ) -> PhysicalResult<execution::AggregateFunction> {
        match self {
            AggregateFunction::Avg => Ok(execution::AggregateFunction::Avg),
            _ => {
                unimplemented!();
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Aggregate {
    pub(crate) aggregate_func: AggregateFunction,
    pub(crate) argument: Option<NamedExpression>,
}

impl Aggregate {
    pub(crate) fn new(aggregate_func: AggregateFunction, argument: Option<NamedExpression>) -> Self {
        Aggregate {
            aggregate_func,
            argument,
        }
    }

    pub(crate) fn physical(
        &self,
        physical_plan_creator: &mut PhysicalPlanCreator,
    ) -> PhysicalResult<(execution::Aggregate, common::Variables)> {
        let aggregate_func = self.aggregate_func.physical(physical_plan_creator)?;

        let mut variables = common::empty_variables();

        let arg = if let Some(named_expr) = &self.argument {
            let (physical_expr, expr_variables) = named_expr.expr.physical(physical_plan_creator)?;
            variables = common::merge(variables, expr_variables);
            Some(*physical_expr)
        } else {
            None
        };
        let aggregate = execution::Aggregate::new(aggregate_func, arg);

        Ok((aggregate, variables))
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
