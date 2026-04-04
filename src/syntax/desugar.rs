use super::ast::*;

/// Post-parse AST transformation pass.
/// Rewrites syntactic sugar nodes into core AST nodes:
/// - BETWEEN(x, lo, hi) → x >= lo AND x <= hi
/// - COALESCE(a, b, ...) → nested CASE WHEN IS NOT NULL/MISSING
/// - NULLIF(a, b) → CASE WHEN a = b THEN NULL ELSE a END
pub(crate) fn desugar_statement(stmt: SelectStatement) -> SelectStatement {
    SelectStatement {
        distinct: stmt.distinct,
        select_clause: desugar_select_clause(stmt.select_clause),
        from_clause: desugar_from_clause(stmt.from_clause),
        where_expr_opt: stmt.where_expr_opt.map(|w| WhereExpression::new(desugar_expr(w.expr))),
        group_by_exprs_opt: stmt.group_by_exprs_opt,
        having_expr_opt: stmt.having_expr_opt.map(|h| WhereExpression::new(desugar_expr(h.expr))),
        order_by_expr_opt: stmt.order_by_expr_opt,
        limit_expr_opt: stmt.limit_expr_opt,
    }
}

fn desugar_from_clause(from: FromClause) -> FromClause {
    match from {
        FromClause::Tables(_) => from,
        FromClause::Join { left, right, join_type, condition } => {
            FromClause::Join {
                left: Box::new(desugar_from_clause(*left)),
                right,
                join_type,
                condition: condition.map(desugar_expr),
            }
        }
    }
}

fn desugar_select_clause(clause: SelectClause) -> SelectClause {
    match clause {
        SelectClause::SelectExpressions(exprs) => {
            SelectClause::SelectExpressions(exprs.into_iter().map(desugar_select_expr).collect())
        }
        other => other,
    }
}

fn desugar_select_expr(se: SelectExpression) -> SelectExpression {
    match se {
        SelectExpression::Expression(expr, alias) => {
            SelectExpression::Expression(Box::new(desugar_expr(*expr)), alias)
        }
        SelectExpression::Star => SelectExpression::Star,
    }
}

/// Recursively walk an expression, rewriting sugar nodes.
/// Rewrites BETWEEN (Step 17), COALESCE/NULLIF (Step 21).
pub(crate) fn desugar_expr(expr: Expression) -> Expression {
    match expr {
        Expression::BinaryOperator(op, l, r) => {
            Expression::BinaryOperator(op, Box::new(desugar_expr(*l)), Box::new(desugar_expr(*r)))
        }
        Expression::UnaryOperator(op, inner) => {
            Expression::UnaryOperator(op, Box::new(desugar_expr(*inner)))
        }
        Expression::CaseWhenExpression(cwe) => {
            let branches = cwe
                .branches
                .into_iter()
                .map(|(cond, then_e)| (desugar_expr(cond), desugar_expr(then_e)))
                .collect();
            let else_expr = cwe.else_expr.map(|e| Box::new(desugar_expr(*e)));
            Expression::CaseWhenExpression(CaseWhenExpression {
                branches,
                else_expr,
            })
        }
        Expression::FuncCall(ref name, ref args, _) if name.eq_ignore_ascii_case("coalesce") => {
            desugar_coalesce(args)
        }
        Expression::FuncCall(ref name, ref args, _) if name.eq_ignore_ascii_case("nullif") => {
            desugar_nullif(args)
        }
        Expression::FuncCall(name, args, within) => {
            let args = args.into_iter().map(desugar_select_expr).collect();
            Expression::FuncCall(name, args, within)
        }
        Expression::IsNull(inner) => Expression::IsNull(Box::new(desugar_expr(*inner))),
        Expression::IsNotNull(inner) => Expression::IsNotNull(Box::new(desugar_expr(*inner))),
        Expression::IsMissing(inner) => Expression::IsMissing(Box::new(desugar_expr(*inner))),
        Expression::IsNotMissing(inner) => {
            Expression::IsNotMissing(Box::new(desugar_expr(*inner)))
        }
        Expression::Like(l, r) => Expression::Like(Box::new(desugar_expr(*l)), Box::new(desugar_expr(*r))),
        Expression::NotLike(l, r) => Expression::NotLike(Box::new(desugar_expr(*l)), Box::new(desugar_expr(*r))),
        Expression::In(expr, list) => {
            Expression::In(Box::new(desugar_expr(*expr)), list.into_iter().map(desugar_expr).collect())
        }
        Expression::NotIn(expr, list) => {
            Expression::NotIn(Box::new(desugar_expr(*expr)), list.into_iter().map(desugar_expr).collect())
        }
        Expression::Between(expr, lo, hi) => {
            // BETWEEN(x, lo, hi) → x >= lo AND x <= hi
            let x = desugar_expr(*expr);
            let lo = desugar_expr(*lo);
            let hi = desugar_expr(*hi);
            Expression::BinaryOperator(
                BinaryOperator::And,
                Box::new(Expression::BinaryOperator(BinaryOperator::GreaterEqual, Box::new(x.clone()), Box::new(lo))),
                Box::new(Expression::BinaryOperator(BinaryOperator::LessEqual, Box::new(x), Box::new(hi))),
            )
        }
        Expression::NotBetween(expr, lo, hi) => {
            // NOT BETWEEN(x, lo, hi) → x < lo OR x > hi
            let x = desugar_expr(*expr);
            let lo = desugar_expr(*lo);
            let hi = desugar_expr(*hi);
            Expression::BinaryOperator(
                BinaryOperator::Or,
                Box::new(Expression::BinaryOperator(BinaryOperator::LessThan, Box::new(x.clone()), Box::new(lo))),
                Box::new(Expression::BinaryOperator(BinaryOperator::MoreThan, Box::new(x), Box::new(hi))),
            )
        }
        Expression::Cast(inner, cast_type) => Expression::Cast(Box::new(desugar_expr(*inner)), cast_type),
        // Leaf nodes — no children to recurse into
        Expression::Column(_) | Expression::Value(_) => expr,
    }
}

/// COALESCE(a, b, c, ...) →
///   CASE WHEN a IS NOT NULL AND a IS NOT MISSING THEN a
///        ELSE COALESCE(b, c, ...) END
/// Base cases: COALESCE() → NULL, COALESCE(a) → a
fn desugar_coalesce(args: &[SelectExpression]) -> Expression {
    let exprs: Vec<Expression> = args
        .iter()
        .filter_map(|se| match se {
            SelectExpression::Expression(e, _) => Some((**e).clone()),
            _ => None,
        })
        .collect();

    if exprs.is_empty() {
        return Expression::Value(Value::Null);
    }

    if exprs.len() == 1 {
        return desugar_expr(exprs.into_iter().next().unwrap());
    }

    let first = desugar_expr(exprs[0].clone());
    let rest_args: Vec<SelectExpression> = exprs[1..]
        .iter()
        .map(|e| SelectExpression::Expression(Box::new(e.clone()), None))
        .collect();
    let rest = desugar_coalesce(&rest_args);

    Expression::CaseWhenExpression(CaseWhenExpression {
        branches: vec![(
            Expression::BinaryOperator(
                BinaryOperator::And,
                Box::new(Expression::IsNotNull(Box::new(first.clone()))),
                Box::new(Expression::IsNotMissing(Box::new(first.clone()))),
            ),
            first,
        )],
        else_expr: Some(Box::new(rest)),
    })
}

/// NULLIF(a, b) → CASE WHEN a = b THEN NULL ELSE a END
fn desugar_nullif(args: &[SelectExpression]) -> Expression {
    let exprs: Vec<Expression> = args
        .iter()
        .filter_map(|se| match se {
            SelectExpression::Expression(e, _) => Some((**e).clone()),
            _ => None,
        })
        .collect();

    if exprs.len() != 2 {
        // Invalid NULLIF — return as-is (will error at execution)
        return Expression::FuncCall("nullif".to_string(), args.to_vec(), None);
    }

    let a = desugar_expr(exprs[0].clone());
    let b = desugar_expr(exprs[1].clone());

    Expression::CaseWhenExpression(CaseWhenExpression {
        branches: vec![(
            Expression::BinaryOperator(
                BinaryOperator::Equal,
                Box::new(a.clone()),
                Box::new(b),
            ),
            Expression::Value(Value::Null),
        )],
        else_expr: Some(Box::new(a)),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_desugar_identity() {
        // Desugaring a simple expression should return it unchanged
        let expr = Expression::Value(Value::Integral(42));
        let result = desugar_expr(expr.clone());
        assert_eq!(result, expr);
    }

    #[test]
    fn test_desugar_nested_expression() {
        let expr = Expression::BinaryOperator(
            BinaryOperator::Plus,
            Box::new(Expression::Value(Value::Integral(1))),
            Box::new(Expression::Value(Value::Integral(2))),
        );
        let result = desugar_expr(expr.clone());
        assert_eq!(result, expr);
    }

    #[test]
    fn test_desugar_between() {
        // x BETWEEN 1 AND 10 → x >= 1 AND x <= 10
        let x = Expression::Column(PathExpr::new(vec![PathSegment::AttrName("x".to_string())]));
        let lo = Expression::Value(Value::Integral(1));
        let hi = Expression::Value(Value::Integral(10));
        let between = Expression::Between(Box::new(x.clone()), Box::new(lo.clone()), Box::new(hi.clone()));
        let result = desugar_expr(between);
        let expected = Expression::BinaryOperator(
            BinaryOperator::And,
            Box::new(Expression::BinaryOperator(BinaryOperator::GreaterEqual, Box::new(x.clone()), Box::new(lo))),
            Box::new(Expression::BinaryOperator(BinaryOperator::LessEqual, Box::new(x), Box::new(hi))),
        );
        assert_eq!(result, expected);
    }

    #[test]
    fn test_desugar_not_between() {
        // x NOT BETWEEN 1 AND 10 → x < 1 OR x > 10
        let x = Expression::Column(PathExpr::new(vec![PathSegment::AttrName("x".to_string())]));
        let lo = Expression::Value(Value::Integral(1));
        let hi = Expression::Value(Value::Integral(10));
        let not_between = Expression::NotBetween(Box::new(x.clone()), Box::new(lo.clone()), Box::new(hi.clone()));
        let result = desugar_expr(not_between);
        let expected = Expression::BinaryOperator(
            BinaryOperator::Or,
            Box::new(Expression::BinaryOperator(BinaryOperator::LessThan, Box::new(x.clone()), Box::new(lo))),
            Box::new(Expression::BinaryOperator(BinaryOperator::MoreThan, Box::new(x), Box::new(hi))),
        );
        assert_eq!(result, expected);
    }

    #[test]
    fn test_desugar_coalesce_two_args() {
        // COALESCE(a, b) → CASE WHEN a IS NOT NULL AND a IS NOT MISSING THEN a ELSE b END
        let a = Expression::Column(PathExpr::new(vec![PathSegment::AttrName("a".to_string())]));
        let b = Expression::Column(PathExpr::new(vec![PathSegment::AttrName("b".to_string())]));
        let coalesce = Expression::FuncCall(
            "coalesce".to_string(),
            vec![
                SelectExpression::Expression(Box::new(a.clone()), None),
                SelectExpression::Expression(Box::new(b.clone()), None),
            ],
            None,
        );
        let result = desugar_expr(coalesce);
        let expected = Expression::CaseWhenExpression(CaseWhenExpression {
            branches: vec![(
                Expression::BinaryOperator(
                    BinaryOperator::And,
                    Box::new(Expression::IsNotNull(Box::new(a.clone()))),
                    Box::new(Expression::IsNotMissing(Box::new(a.clone()))),
                ),
                a,
            )],
            else_expr: Some(Box::new(b)),
        });
        assert_eq!(result, expected);
    }

    #[test]
    fn test_desugar_coalesce_three_args() {
        // COALESCE(a, b, c) → nested CASE WHEN
        let a = Expression::Column(PathExpr::new(vec![PathSegment::AttrName("a".to_string())]));
        let b = Expression::Column(PathExpr::new(vec![PathSegment::AttrName("b".to_string())]));
        let c = Expression::Column(PathExpr::new(vec![PathSegment::AttrName("c".to_string())]));
        let coalesce = Expression::FuncCall(
            "coalesce".to_string(),
            vec![
                SelectExpression::Expression(Box::new(a.clone()), None),
                SelectExpression::Expression(Box::new(b.clone()), None),
                SelectExpression::Expression(Box::new(c.clone()), None),
            ],
            None,
        );
        let result = desugar_expr(coalesce);

        // Should be CASE WHEN a IS NOT NULL AND a IS NOT MISSING THEN a
        //   ELSE CASE WHEN b IS NOT NULL AND b IS NOT MISSING THEN b ELSE c END END
        let inner = Expression::CaseWhenExpression(CaseWhenExpression {
            branches: vec![(
                Expression::BinaryOperator(
                    BinaryOperator::And,
                    Box::new(Expression::IsNotNull(Box::new(b.clone()))),
                    Box::new(Expression::IsNotMissing(Box::new(b.clone()))),
                ),
                b,
            )],
            else_expr: Some(Box::new(c)),
        });
        let expected = Expression::CaseWhenExpression(CaseWhenExpression {
            branches: vec![(
                Expression::BinaryOperator(
                    BinaryOperator::And,
                    Box::new(Expression::IsNotNull(Box::new(a.clone()))),
                    Box::new(Expression::IsNotMissing(Box::new(a.clone()))),
                ),
                a,
            )],
            else_expr: Some(Box::new(inner)),
        });
        assert_eq!(result, expected);
    }

    #[test]
    fn test_desugar_coalesce_single_arg() {
        // COALESCE(a) → a
        let a = Expression::Column(PathExpr::new(vec![PathSegment::AttrName("a".to_string())]));
        let coalesce = Expression::FuncCall(
            "coalesce".to_string(),
            vec![SelectExpression::Expression(Box::new(a.clone()), None)],
            None,
        );
        let result = desugar_expr(coalesce);
        assert_eq!(result, a);
    }

    #[test]
    fn test_desugar_coalesce_no_args() {
        // COALESCE() → NULL
        let coalesce = Expression::FuncCall("coalesce".to_string(), vec![], None);
        let result = desugar_expr(coalesce);
        assert_eq!(result, Expression::Value(Value::Null));
    }

    #[test]
    fn test_desugar_coalesce_case_insensitive() {
        // COALESCE should be recognized case-insensitively
        let a = Expression::Column(PathExpr::new(vec![PathSegment::AttrName("a".to_string())]));
        let b = Expression::Column(PathExpr::new(vec![PathSegment::AttrName("b".to_string())]));
        let coalesce = Expression::FuncCall(
            "COALESCE".to_string(),
            vec![
                SelectExpression::Expression(Box::new(a.clone()), None),
                SelectExpression::Expression(Box::new(b.clone()), None),
            ],
            None,
        );
        let result = desugar_expr(coalesce);
        match result {
            Expression::CaseWhenExpression(_) => {} // desugared correctly
            other => panic!("Expected CaseWhenExpression, got {:?}", other),
        }
    }

    #[test]
    fn test_desugar_nullif() {
        // NULLIF(a, b) → CASE WHEN a = b THEN NULL ELSE a END
        let a = Expression::Column(PathExpr::new(vec![PathSegment::AttrName("a".to_string())]));
        let b = Expression::Column(PathExpr::new(vec![PathSegment::AttrName("b".to_string())]));
        let nullif = Expression::FuncCall(
            "nullif".to_string(),
            vec![
                SelectExpression::Expression(Box::new(a.clone()), None),
                SelectExpression::Expression(Box::new(b.clone()), None),
            ],
            None,
        );
        let result = desugar_expr(nullif);
        let expected = Expression::CaseWhenExpression(CaseWhenExpression {
            branches: vec![(
                Expression::BinaryOperator(
                    BinaryOperator::Equal,
                    Box::new(a.clone()),
                    Box::new(b),
                ),
                Expression::Value(Value::Null),
            )],
            else_expr: Some(Box::new(a)),
        });
        assert_eq!(result, expected);
    }

    #[test]
    fn test_desugar_nullif_case_insensitive() {
        // NULLIF should be recognized case-insensitively
        let a = Expression::Column(PathExpr::new(vec![PathSegment::AttrName("a".to_string())]));
        let b = Expression::Column(PathExpr::new(vec![PathSegment::AttrName("b".to_string())]));
        let nullif = Expression::FuncCall(
            "NULLIF".to_string(),
            vec![
                SelectExpression::Expression(Box::new(a.clone()), None),
                SelectExpression::Expression(Box::new(b.clone()), None),
            ],
            None,
        );
        let result = desugar_expr(nullif);
        match result {
            Expression::CaseWhenExpression(_) => {} // desugared correctly
            other => panic!("Expected CaseWhenExpression, got {:?}", other),
        }
    }

    #[test]
    fn test_desugar_nullif_wrong_arg_count() {
        // NULLIF with wrong number of args should remain as FuncCall
        let a = Expression::Column(PathExpr::new(vec![PathSegment::AttrName("a".to_string())]));
        let nullif = Expression::FuncCall(
            "nullif".to_string(),
            vec![SelectExpression::Expression(Box::new(a.clone()), None)],
            None,
        );
        let result = desugar_expr(nullif);
        match result {
            Expression::FuncCall(name, _, _) => assert_eq!(name, "nullif"),
            other => panic!("Expected FuncCall, got {:?}", other),
        }
    }
}
