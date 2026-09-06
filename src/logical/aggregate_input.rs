//! Bind aggregates to their projected arguments without overwriting grouping keys.

use super::types::{Aggregate, Expression, Named};
use crate::syntax::ast::{PathExpr, PathSegment, SelectStatement};
use hashbrown::HashSet;

pub(super) struct InputSlots {
    reserved: HashSet<String>,
    next: usize,
}

impl InputSlots {
    pub(super) fn new(query: &SelectStatement) -> Self {
        Self {
            reserved: super::having::reserved_names(query),
            next: 1,
        }
    }

    /// Nested/aliased arguments must read their materialized output column.
    /// Private names also protect earlier, different inputs from alias collisions.
    /// The caller inserts the projection at its original SELECT-list position.
    pub(super) fn bind_projection(&mut self, aggregate: &mut Aggregate, projections: &[Named]) -> Option<Named> {
        let (expression, alias) = match aggregate {
            Aggregate::Avg(Named::Expression(expression, alias))
            | Aggregate::Count(Named::Expression(expression, alias))
            | Aggregate::Sum(Named::Expression(expression, alias))
            | Aggregate::Min(Named::Expression(expression, alias))
            | Aggregate::Max(Named::Expression(expression, alias))
            | Aggregate::First(Named::Expression(expression, alias))
            | Aggregate::Last(Named::Expression(expression, alias))
            | Aggregate::ApproxCountDistinct(Named::Expression(expression, alias)) => {
                (expression.clone(), alias.clone())
            }
            Aggregate::PercentileDisc(_, path, _) | Aggregate::ApproxPercentile(_, path, _) => {
                (Expression::Variable(path.clone()), Some(path.unwrap_last()))
            }
            _ => return None,
        };
        let output = alias.unwrap_or_else(|| format!("_{}", projections.len()));
        let collision = projections.iter().enumerate().any(|(index, previous)| {
            let Named::Expression(previous, alias) = previous else {
                return false;
            };
            let previous_output = alias.clone().unwrap_or_else(|| format!("_{index}"));
            previous_output == output && (previous != &expression || !matches!(expression, Expression::Variable(_)))
        });
        let needs_variable_binding = matches!(&expression, Expression::Variable(path)
            if !matches!(path.path_segments.as_slice(), [PathSegment::AttrName(name)] if name == &output));
        if !collision && !needs_variable_binding {
            return None;
        }

        let bound_output = if collision {
            loop {
                let candidate = format!("__logq_aggregate_input_{}", self.next);
                self.next += 1;
                if self.reserved.insert(candidate.clone()) {
                    break candidate;
                }
            }
        } else {
            output
        };
        let path = PathExpr::new(vec![PathSegment::AttrName(bound_output.clone())]);
        match aggregate {
            Aggregate::Avg(input)
            | Aggregate::Count(input)
            | Aggregate::Sum(input)
            | Aggregate::Min(input)
            | Aggregate::Max(input)
            | Aggregate::First(input)
            | Aggregate::Last(input)
            | Aggregate::ApproxCountDistinct(input) => {
                *input = Named::Expression(Expression::Variable(path), Some(bound_output.clone()));
            }
            Aggregate::PercentileDisc(_, input, _) | Aggregate::ApproxPercentile(_, input, _) => *input = path,
            _ => unreachable!("only supported aggregate arguments can collide"),
        }
        Some(Named::Expression(expression, Some(bound_output)))
    }
}
