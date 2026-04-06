// src/logical/optimizer.rs

use crate::logical::types::{Formula, LogicInfixOp, Expression};

/// Extract AND conjuncts from a nested InfixOperator(And, ...) tree into a flat list.
pub(crate) fn extract_conjuncts(formula: Formula) -> Vec<Formula> {
    let mut result = Vec::new();
    extract_conjuncts_inner(formula, &mut result);
    result
}

fn extract_conjuncts_inner(formula: Formula, out: &mut Vec<Formula>) {
    match formula {
        Formula::InfixOperator(LogicInfixOp::And, left, right) => {
            extract_conjuncts_inner(*left, out);
            extract_conjuncts_inner(*right, out);
        }
        other => out.push(other),
    }
}

/// Rebuild a conjunction (AND tree) from a flat list of conjuncts.
/// Folds right: [a, b, c] -> And(a, And(b, c)).
pub(crate) fn rebuild_conjunction(mut conjuncts: Vec<Formula>) -> Formula {
    assert!(!conjuncts.is_empty(), "cannot rebuild empty conjunction");
    if conjuncts.len() == 1 {
        return conjuncts.pop().unwrap();
    }
    let mut result = conjuncts.pop().unwrap();
    while let Some(f) = conjuncts.pop() {
        result = Formula::InfixOperator(LogicInfixOp::And, Box::new(f), Box::new(result));
    }
    result
}

/// Estimated cost of evaluating a predicate. Lower = evaluate first.
pub(crate) fn predicate_cost(formula: &Formula) -> u32 {
    match formula {
        Formula::IsNull(_) | Formula::IsNotNull(_)
        | Formula::IsMissing(_) | Formula::IsNotMissing(_) => 1,

        Formula::Constant(_) => 0,

        Formula::Predicate(_, left, right) => {
            if involves_function(left) || involves_function(right) {
                100
            } else {
                10 // numeric or simple comparison
            }
        }

        Formula::Like(_, _) | Formula::NotLike(_, _) => 50,

        Formula::In(_, _) | Formula::NotIn(_, _) => 40,

        Formula::PrefixOperator(_, inner) => predicate_cost(inner),

        Formula::InfixOperator(_, left, right) => {
            predicate_cost(left).max(predicate_cost(right))
        }

        Formula::ExpressionPredicate(_) => 60,
    }
}

fn involves_function(expr: &Expression) -> bool {
    matches!(expr, Expression::Function(_, _))
}

/// Reorder AND conjuncts by ascending cost. Non-AND formulas are returned unchanged.
pub(crate) fn reorder_and_conjuncts(formula: Formula) -> Formula {
    match &formula {
        Formula::InfixOperator(LogicInfixOp::And, _, _) => {
            let mut conjuncts = extract_conjuncts(formula);
            conjuncts.sort_by_key(|f| predicate_cost(f));
            rebuild_conjunction(conjuncts)
        }
        _ => formula,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::logical::types::{Formula, Expression, LogicInfixOp, Relation};
    use crate::common::types::Value;
    use crate::syntax::ast::{PathExpr, PathSegment};

    fn var(name: &str) -> Box<Expression> {
        Box::new(Expression::Variable(PathExpr::new(vec![
            PathSegment::AttrName(name.to_string()),
        ])))
    }

    fn int_const(v: i32) -> Box<Expression> {
        Box::new(Expression::Constant(Value::Int(v)))
    }

    fn str_const(v: &str) -> Box<Expression> {
        Box::new(Expression::Constant(Value::String(v.to_string())))
    }

    #[test]
    fn test_extract_conjuncts_single() {
        let f = Formula::Predicate(Relation::Equal, var("a"), int_const(1));
        let conjuncts = extract_conjuncts(f);
        assert_eq!(conjuncts.len(), 1);
    }

    #[test]
    fn test_extract_conjuncts_nested_and() {
        let a = Formula::Predicate(Relation::Equal, var("a"), int_const(1));
        let b = Formula::Predicate(Relation::MoreThan, var("b"), int_const(2));
        let c = Formula::IsNull(var("c"));
        let and_bc = Formula::InfixOperator(LogicInfixOp::And, Box::new(b), Box::new(c));
        let and_abc = Formula::InfixOperator(LogicInfixOp::And, Box::new(a), Box::new(and_bc));
        let conjuncts = extract_conjuncts(and_abc);
        assert_eq!(conjuncts.len(), 3);
    }

    #[test]
    fn test_rebuild_conjunction() {
        let a = Formula::IsNull(var("a"));
        let b = Formula::IsNull(var("b"));
        let rebuilt = rebuild_conjunction(vec![a, b]);
        match rebuilt {
            Formula::InfixOperator(LogicInfixOp::And, _, _) => {}
            _ => panic!("expected And"),
        }
    }

    #[test]
    fn test_rebuild_conjunction_single() {
        let a = Formula::IsNull(var("a"));
        let rebuilt = rebuild_conjunction(vec![a]);
        match rebuilt {
            Formula::IsNull(_) => {}
            _ => panic!("expected single IsNull"),
        }
    }

    #[test]
    fn test_reorder_puts_isnull_before_predicate() {
        let expensive = Formula::Like(var("user_agent"), str_const("%bot%"));
        let cheap = Formula::IsNull(var("a"));
        let and_expr = Formula::InfixOperator(
            LogicInfixOp::And,
            Box::new(expensive),
            Box::new(cheap),
        );
        let reordered = reorder_and_conjuncts(and_expr);
        let conjuncts = extract_conjuncts(reordered);
        // IsNull (cost 1) should come before Like (cost 50)
        assert!(matches!(&conjuncts[0], Formula::IsNull(_)));
    }
}
