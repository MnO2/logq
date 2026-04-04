use super::ast::*;

/// Post-parse AST transformation pass.
/// Rewrites syntactic sugar nodes into core AST nodes:
/// - BETWEEN(x, lo, hi) → x >= lo AND x <= hi
/// - COALESCE(a, b, ...) → nested CASE WHEN IS NOT NULL/MISSING
/// - NULLIF(a, b) → CASE WHEN a = b THEN NULL ELSE a END
pub(crate) fn desugar_statement(stmt: SelectStatement) -> SelectStatement {
    SelectStatement {
        select_clause: desugar_select_clause(stmt.select_clause),
        table_references: stmt.table_references,
        where_expr_opt: stmt.where_expr_opt.map(|w| WhereExpression::new(desugar_expr(w.expr))),
        group_by_exprs_opt: stmt.group_by_exprs_opt,
        having_expr_opt: stmt.having_expr_opt.map(|h| WhereExpression::new(desugar_expr(h.expr))),
        order_by_expr_opt: stmt.order_by_expr_opt,
        limit_expr_opt: stmt.limit_expr_opt,
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
/// Currently identity — populated by BETWEEN (Step 17) and COALESCE/NULLIF (Step 21).
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
        // Leaf nodes — no children to recurse into
        Expression::Column(_) | Expression::Value(_) => expr,
    }
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
}
