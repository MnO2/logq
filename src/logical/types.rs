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
    Map(Vec<Named>, Box<Node>),
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
                let mut physical_expressions: Vec<execution::Named> = Vec::new();
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
pub(crate) enum Named {
    Expression(Expression, VariableName),
    Star,
}

impl Named {
    pub(crate) fn physical(
        &self,
        physical_plan_creator: &mut PhysicalPlanCreator,
    ) -> PhysicalResult<(Box<execution::Named>, common::Variables)> {
        match self {
            Named::Expression(expr, name) => {
                let (physical_expr, expr_variables) = expr.physical(physical_plan_creator)?;
                Ok((
                    Box::new(execution::Named::Expression(*physical_expr, name.clone())),
                    expr_variables,
                ))
            }
            Named::Star => Ok((Box::new(execution::Named::Star), common::empty_variables())),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Expression {
    Constant(common::Value),
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
            Expression::Constant(value) => {
                let constant_name = physical_plan_creator.new_constant_name();
                let node = Box::new(execution::Expression::Variable(constant_name.clone()));
                let mut variables = common::Variables::default();
                variables.insert(constant_name, value.clone());

                Ok((node, variables))
            }
            Expression::Variable(name) => {
                let node = Box::new(execution::Expression::Variable(name.clone()));
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
    InfixOperator(LogicInfixOp, Box<Formula>, Box<Formula>),
    PrefixOperator(LogicPrefixOp, Box<Formula>),
    Constant(bool),
    Predicate(Relation, Box<Expression>, Box<Expression>),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum LogicInfixOp {
    And,
    Or,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum LogicPrefixOp {
    Not,
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

                match op {
                    LogicInfixOp::And => Ok((Box::new(execution::Formula::And(left, right)), return_variables)),
                    LogicInfixOp::Or => Ok((Box::new(execution::Formula::Or(left, right)), return_variables)),
                }
            }
            Formula::PrefixOperator(op, child_formula) => match op {
                LogicPrefixOp::Not => {
                    let (child, child_variables) = child_formula.physical(physical_plan_creator)?;
                    Ok((Box::new(execution::Formula::Not(child)), child_variables))
                }
            },
            Formula::Constant(b) => {
                let node = Box::new(execution::Formula::Constant(*b));
                let variables = common::Variables::default();

                Ok((node, variables))
            }
            Formula::Predicate(relation, left_expr, right_expr) => {
                let (left, left_variables) = left_expr.physical(physical_plan_creator)?;
                let (right, right_variables) = right_expr.physical(physical_plan_creator)?;
                let physical_relation = relation.physical()?;

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
    counter: u32,
    data_source: execution::Node,
}

impl PhysicalPlanCreator {
    pub(crate) fn new(name: String) -> Self {
        let data_source = execution::Node::DataSource(name);
        PhysicalPlanCreator {
            counter: 0,
            data_source,
        }
    }

    pub(crate) fn new_constant_name(&mut self) -> VariableName {
        let constant_name = format!("const_{:09}", self.counter);
        self.counter += 1;
        constant_name
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
    pub(crate) fn physical(&self) -> PhysicalResult<execution::AggregateFunction> {
        match self {
            AggregateFunction::Avg => Ok(execution::AggregateFunction::Avg),
            AggregateFunction::Count => Ok(execution::AggregateFunction::Count),
            AggregateFunction::First => Ok(execution::AggregateFunction::First),
            AggregateFunction::Last => Ok(execution::AggregateFunction::Last),
            AggregateFunction::Max => Ok(execution::AggregateFunction::Max),
            AggregateFunction::Min => Ok(execution::AggregateFunction::Min),
            AggregateFunction::Sum => Ok(execution::AggregateFunction::Sum),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Aggregate {
    pub(crate) aggregate_func: AggregateFunction,
    pub(crate) argument: Named,
}

impl Aggregate {
    pub(crate) fn new(aggregate_func: AggregateFunction, argument: Named) -> Self {
        Aggregate {
            aggregate_func,
            argument,
        }
    }

    pub(crate) fn physical(
        &self,
        physical_plan_creator: &mut PhysicalPlanCreator,
    ) -> PhysicalResult<(execution::Aggregate, common::Variables)> {
        let aggregate_func = self.aggregate_func.physical()?;

        let mut variables = common::empty_variables();

        let arg = match &self.argument {
            Named::Expression(expr, name) => {
                let (physical_expr, expr_variables) = expr.physical(physical_plan_creator)?;
                variables = common::merge(variables, expr_variables);
                execution::Named::Expression(*physical_expr, name.clone())
            }
            Named::Star => execution::Named::Star,
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
    pub(crate) fn physical(&self) -> PhysicalResult<execution::Relation> {
        match self {
            Relation::Equal => Ok(execution::Relation::Equal),
            Relation::NotEqual => Ok(execution::Relation::NotEqual),
            Relation::MoreThan => Ok(execution::Relation::MoreThan),
            Relation::LessThan => Ok(execution::Relation::LessThan),
            Relation::GreaterEqual => Ok(execution::Relation::GreaterEqual),
            Relation::LessEqual => Ok(execution::Relation::LessEqual),
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_relation_gen_physical() {
        let rel = Relation::Equal;
        let ans = rel.physical().unwrap();
        let expected = execution::Relation::Equal;

        assert_eq!(expected, ans);
    }

    #[test]
    fn test_aggregate_function_gen_physical() {
        let aggregate_function = AggregateFunction::Avg;
        let ans = aggregate_function.physical().unwrap();
        let expected = execution::AggregateFunction::Avg;

        assert_eq!(expected, ans);

        let aggregate_function = AggregateFunction::Count;
        let ans = aggregate_function.physical().unwrap();
        let expected = execution::AggregateFunction::Count;

        assert_eq!(expected, ans);
    }

    #[test]
    fn test_formula_gen_physical() {
        let formula = Formula::InfixOperator(
            LogicInfixOp::And,
            Box::new(Formula::Constant(true)),
            Box::new(Formula::Constant(false)),
        );
        let mut physical_plan_creator = PhysicalPlanCreator::new("filename".to_string());
        let (physical_formula, variables) = formula.physical(&mut physical_plan_creator).unwrap();
        let expected_formula = execution::Formula::And(
            Box::new(execution::Formula::Constant(true)),
            Box::new(execution::Formula::Constant(false)),
        );

        let expected_variables = common::Variables::default();

        assert_eq!(expected_formula, *physical_formula);
        assert_eq!(expected_variables, variables);
    }

    #[test]
    fn test_expression_gen_physical() {
        let expr = Expression::Constant(common::Value::Int(1));
        let mut physical_plan_creator = PhysicalPlanCreator::new("filename".to_string());
        let (physical_expr, variables) = expr.physical(&mut physical_plan_creator).unwrap();
        let expected_formula = execution::Expression::Variable("const_000000000".to_string());

        let mut expected_variables = common::Variables::default();
        expected_variables.insert("const_000000000".to_string(), common::Value::Int(1));
        assert_eq!(expected_formula, *physical_expr);
        assert_eq!(expected_variables, variables);
    }

    #[test]
    fn test_filter_with_map_gen_physical() {
        let filtered_formula = Formula::Predicate(
            Relation::Equal,
            Box::new(Expression::Variable("a".to_string())),
            Box::new(Expression::Constant(common::Value::Int(1))),
        );

        let filter = Node::Filter(
            Box::new(filtered_formula),
            Box::new(Node::Map(
                vec![
                    Named::Expression(Expression::Variable("a".to_string()), "a".to_string()),
                    Named::Expression(Expression::Variable("b".to_string()), "b".to_string()),
                ],
                Box::new(Node::DataSource(DataSource::File(
                    std::path::Path::new("filename").to_path_buf(),
                ))),
            )),
        );

        let mut physical_plan_creator = PhysicalPlanCreator::new("a".to_string());
        let (physical_formula, variables) = filter.physical(&mut physical_plan_creator).unwrap();

        let expected_filtered_formula = execution::Formula::Predicate(
            execution::Relation::Equal,
            Box::new(execution::Expression::Variable("a".to_string())),
            Box::new(execution::Expression::Variable("const_000000000".to_string())),
        );

        let expected_source = execution::Node::Map(
            vec![
                execution::Named::Expression(execution::Expression::Variable("a".to_string()), "a".to_string()),
                execution::Named::Expression(execution::Expression::Variable("b".to_string()), "b".to_string()),
            ],
            Box::new(execution::Node::DataSource("a".to_string())),
        );

        let expected_filter = execution::Node::Filter(Box::new(expected_source), Box::new(expected_filtered_formula));

        let mut expected_variables = common::Variables::default();
        expected_variables.insert("const_000000000".to_string(), common::Value::Int(1));

        assert_eq!(expected_filter, *physical_formula);
        assert_eq!(expected_variables, variables);
    }

    #[test]
    fn test_group_by_gen_physical() {
        let filtered_formula = Formula::Predicate(
            Relation::Equal,
            Box::new(Expression::Variable("a".to_string())),
            Box::new(Expression::Constant(common::Value::Int(1))),
        );

        let filter = Node::Filter(
            Box::new(filtered_formula),
            Box::new(Node::Map(
                vec![
                    Named::Expression(Expression::Variable("a".to_string()), "a".to_string()),
                    Named::Expression(Expression::Variable("b".to_string()), "b".to_string()),
                ],
                Box::new(Node::DataSource(DataSource::File(
                    std::path::Path::new("filename").to_path_buf(),
                ))),
            )),
        );

        let aggregates = vec![
            Aggregate::new(
                AggregateFunction::Avg,
                Named::Expression(Expression::Variable("a".to_string()), "a".to_string()),
            ),
            Aggregate::new(
                AggregateFunction::Count,
                Named::Expression(Expression::Variable("b".to_string()), "b".to_string()),
            ),
        ];

        let fields = vec!["b".to_string()];
        let group_by = Node::GroupBy(fields, aggregates, Box::new(filter));

        let mut physical_plan_creator = PhysicalPlanCreator::new("a".to_string());
        let (physical_formula, variables) = group_by.physical(&mut physical_plan_creator).unwrap();

        let expected_filtered_formula = execution::Formula::Predicate(
            execution::Relation::Equal,
            Box::new(execution::Expression::Variable("a".to_string())),
            Box::new(execution::Expression::Variable("const_000000000".to_string())),
        );

        let expected_source = execution::Node::Map(
            vec![
                execution::Named::Expression(execution::Expression::Variable("a".to_string()), "a".to_string()),
                execution::Named::Expression(execution::Expression::Variable("b".to_string()), "b".to_string()),
            ],
            Box::new(execution::Node::DataSource("a".to_string())),
        );

        let expected_filter = execution::Node::Filter(Box::new(expected_source), Box::new(expected_filtered_formula));
        let expected_group_by = execution::Node::GroupBy(
            vec!["b".to_string()],
            vec![
                execution::Aggregate::new(
                    execution::AggregateFunction::Avg,
                    execution::Named::Expression(execution::Expression::Variable("a".to_string()), "a".to_string()),
                ),
                execution::Aggregate::new(
                    execution::AggregateFunction::Count,
                    execution::Named::Expression(execution::Expression::Variable("b".to_string()), "b".to_string()),
                ),
            ],
            Box::new(expected_filter),
        );

        let mut expected_variables = common::Variables::default();
        expected_variables.insert("const_000000000".to_string(), common::Value::Int(1));

        assert_eq!(expected_group_by, *physical_formula);
        assert_eq!(expected_variables, variables);
    }
}
