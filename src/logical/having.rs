//! Bind HAVING aggregates to GroupBy outputs before ordinary expression planning.

use super::parser::{ParseError, ParseResult, is_aggregate_name};
use super::types;
use crate::syntax::ast::{self, Expression, PathExpr, PathSegment, SelectExpression};
use hashbrown::{HashMap, HashSet};

fn visit(expression: &Expression, visitor: &mut impl FnMut(&Expression)) {
    visitor(expression);
    match expression {
        Expression::BinaryOperator(_, left, right)
        | Expression::Like(left, right)
        | Expression::NotLike(left, right) => {
            visit(left, visitor);
            visit(right, visitor);
        }
        Expression::UnaryOperator(_, inner)
        | Expression::IsNull(inner)
        | Expression::IsNotNull(inner)
        | Expression::IsMissing(inner)
        | Expression::IsNotMissing(inner)
        | Expression::Cast(inner, _) => visit(inner, visitor),
        Expression::In(inner, values) | Expression::NotIn(inner, values) => {
            visit(inner, visitor);
            for value in values {
                visit(value, visitor);
            }
        }
        Expression::Between(value, lower, upper) | Expression::NotBetween(value, lower, upper) => {
            for child in [value, lower, upper] {
                visit(child, visitor);
            }
        }
        Expression::FuncCall(_, args, _) => {
            for argument in args {
                if let SelectExpression::Expression(inner, _) = argument {
                    visit(inner, visitor);
                }
            }
        }
        Expression::CaseWhenExpression(case) => {
            for (condition, value) in &case.branches {
                visit(condition, visitor);
                visit(value, visitor);
            }
            if let Some(value) = &case.else_expr {
                visit(value, visitor);
            }
        }
        // A subquery has its own grouping scope and is planned independently.
        Expression::Subquery(_) | Expression::Column(_) | Expression::Value(_) => {}
    }
}

fn reserve_path(path: &PathExpr, reserved: &mut HashSet<String>) {
    for segment in &path.path_segments {
        if let PathSegment::AttrName(name) | PathSegment::ArrayIndex(name, _) = segment {
            reserved.insert(name.clone());
        }
    }
}

fn reserve_expression(expression: &Expression, reserved: &mut HashSet<String>) {
    visit(expression, &mut |expression| match expression {
        Expression::Column(path) => reserve_path(path, reserved),
        Expression::FuncCall(_, args, within) => {
            for arg in args {
                if let SelectExpression::Expression(_, Some(alias)) = arg {
                    reserved.insert(alias.clone());
                }
            }
            if let Some(within) = within {
                reserve_path(&within.ordering_term.column_name, reserved);
            }
        }
        _ => {}
    });
}

pub(super) fn reserved_names(query: &ast::SelectStatement) -> HashSet<String> {
    let mut reserved = HashSet::new();
    if let ast::SelectClause::SelectExpressions(selected) = &query.select_clause {
        for selected in selected {
            if let SelectExpression::Expression(expression, alias) = selected {
                reserve_expression(expression, &mut reserved);
                if let Some(alias) = alias {
                    reserved.insert(alias.clone());
                }
            }
        }
    }
    if let Some(group) = &query.group_by_exprs_opt {
        for reference in &group.exprs {
            reserve_expression(&reference.column_expr, &mut reserved);
            if let Some(alias) = &reference.as_clause {
                reserved.insert(alias.clone());
            }
        }
        if let Some(alias) = &group.group_as_clause {
            reserved.insert(alias.clone());
        }
    }
    for filter in [&query.where_expr_opt, &query.having_expr_opt].into_iter().flatten() {
        reserve_expression(&filter.expr, &mut reserved);
    }
    if let Some(order) = &query.order_by_expr_opt {
        for term in &order.ordering_terms {
            reserve_path(&term.column_name, &mut reserved);
        }
    }
    for reference in query.from_clause.collect_table_references() {
        reserve_path(&reference.path_expr, &mut reserved);
        for alias in [&reference.as_clause, &reference.at_clause].into_iter().flatten() {
            reserved.insert(alias.clone());
        }
    }
    fn reserve_join_conditions(from: &ast::FromClause, reserved: &mut HashSet<String>) {
        if let ast::FromClause::Join { left, condition, .. } = from {
            reserve_join_conditions(left, reserved);
            if let Some(condition) = condition {
                reserve_expression(condition, reserved);
            }
        }
    }
    reserve_join_conditions(&query.from_clause, &mut reserved);
    reserved
}

fn canonical_aggregate(expression: &Expression) -> Expression {
    let mut expression = expression.clone();
    if let Expression::FuncCall(name, _, _) = &mut expression {
        name.make_ascii_lowercase();
    }
    expression
}

struct Binder {
    bindings: Vec<(Expression, String)>,
    reserved: HashSet<String>,
    hidden: Vec<String>,
    added: Vec<SelectExpression>,
    referenced: HashSet<String>,
}

impl Binder {
    fn bind(&mut self, expression: &mut Expression) -> ParseResult<()> {
        if let Expression::FuncCall(name, args, _) = expression {
            if is_aggregate_name(name) {
                for argument in args.iter() {
                    if let SelectExpression::Expression(inner, _) = argument {
                        let mut nested = false;
                        visit(inner, &mut |child| {
                            if matches!(child, Expression::FuncCall(name, _, _) if is_aggregate_name(name)) {
                                nested = true;
                            }
                        });
                        if nested {
                            return Err(ParseError::InvalidArguments(
                                "nested aggregate functions are not supported".into(),
                            ));
                        }
                    }
                }
                let canonical = canonical_aggregate(expression);
                let name = if let Some((_, name)) = self.bindings.iter().find(|(bound, _)| bound == &canonical) {
                    name.clone()
                } else {
                    // All user paths and aliases are reserved; physical literals
                    // are constants and introduce no competing variable names.
                    let mut index = self.hidden.len() + 1;
                    let name = loop {
                        let candidate = format!("__logq_having_{index}");
                        if self.reserved.insert(candidate.clone()) {
                            break candidate;
                        }
                        index += 1;
                    };
                    self.bindings.push((canonical, name.clone()));
                    self.hidden.push(name.clone());
                    self.added.push(SelectExpression::Expression(
                        Box::new(expression.clone()),
                        Some(name.clone()),
                    ));
                    name
                };
                self.referenced.insert(name.clone());
                *expression = Expression::Column(PathExpr::new(vec![PathSegment::AttrName(name)]));
                return Ok(());
            }
        }
        match expression {
            Expression::BinaryOperator(_, left, right)
            | Expression::Like(left, right)
            | Expression::NotLike(left, right) => {
                self.bind(left)?;
                self.bind(right)?;
            }
            Expression::UnaryOperator(_, inner)
            | Expression::IsNull(inner)
            | Expression::IsNotNull(inner)
            | Expression::IsMissing(inner)
            | Expression::IsNotMissing(inner)
            | Expression::Cast(inner, _) => self.bind(inner)?,
            Expression::In(inner, values) | Expression::NotIn(inner, values) => {
                self.bind(inner)?;
                for value in values {
                    self.bind(value)?;
                }
            }
            Expression::Between(value, lower, upper) | Expression::NotBetween(value, lower, upper) => {
                for child in [value, lower, upper] {
                    self.bind(child)?;
                }
            }
            Expression::FuncCall(_, args, _) => {
                for argument in args {
                    if let SelectExpression::Expression(inner, _) = argument {
                        self.bind(inner)?;
                    }
                }
            }
            Expression::CaseWhenExpression(case) => {
                for (condition, value) in &mut case.branches {
                    self.bind(condition)?;
                    self.bind(value)?;
                }
                if let Some(value) = &mut case.else_expr {
                    self.bind(value)?;
                }
            }
            Expression::Subquery(_) | Expression::Column(_) | Expression::Value(_) => {}
        }
        Ok(())
    }
}

#[derive(Default)]
pub(super) struct HavingRewrite {
    pub hidden: Vec<String>,
    pub referenced: HashSet<String>,
}

pub(super) fn rewrite(query: &mut ast::SelectStatement) -> ParseResult<HavingRewrite> {
    let Some(having) = &query.having_expr_opt else {
        return Ok(HavingRewrite::default());
    };
    let ast::SelectClause::SelectExpressions(selected) = &query.select_clause else {
        return Ok(HavingRewrite::default());
    };
    let mut reserved = reserved_names(query);
    let mut output_counts: HashMap<String, usize> = HashMap::new();
    let mut bindings = Vec::new();
    let group_count = query.group_by_exprs_opt.as_ref().map_or(0, |group| group.exprs.len());
    if let Some(group) = &query.group_by_exprs_opt {
        for (position, reference) in group.exprs.iter().enumerate() {
            reserve_expression(&reference.column_expr, &mut reserved);
            let name = reference
                .as_clause
                .clone()
                .unwrap_or_else(|| match &reference.column_expr {
                    Expression::Column(path) if matches!(path.path_segments.last(), Some(PathSegment::AttrName(_))) => {
                        path.unwrap_last()
                    }
                    _ => format!("_{}", position + 1),
                });
            reserved.insert(name.clone());
            *output_counts.entry(name).or_default() += 1;
        }
        if let Some(name) = &group.group_as_clause {
            reserved.insert(name.clone());
        }
    }
    for selected in selected {
        if let SelectExpression::Expression(expression, alias) = selected {
            reserve_expression(expression, &mut reserved);
            if let Some(alias) = alias {
                reserved.insert(alias.clone());
            }
            if matches!(&**expression, Expression::FuncCall(name, _, _) if is_aggregate_name(name)) {
                let name = alias
                    .clone()
                    .unwrap_or_else(|| format!("_{}", group_count + bindings.len() + 1));
                *output_counts.entry(name.clone()).or_default() += 1;
                bindings.push((canonical_aggregate(expression), name));
            }
        }
    }
    reserve_expression(&having.expr, &mut reserved);
    if let Some(filter) = &query.where_expr_opt {
        reserve_expression(&filter.expr, &mut reserved);
    }
    if let Some(order) = &query.order_by_expr_opt {
        for term in &order.ordering_terms {
            reserve_path(&term.column_name, &mut reserved);
        }
    }
    for reference in query.from_clause.collect_table_references() {
        reserve_path(&reference.path_expr, &mut reserved);
        for alias in [&reference.as_clause, &reference.at_clause].into_iter().flatten() {
            reserved.insert(alias.clone());
        }
    }
    // A duplicate output alias may hold a different aggregate or a group key.
    // Reusing it would bind HAVING to the value that happens to overwrite it.
    let has_selected_aggregate = !bindings.is_empty();
    bindings.retain(|(_, name)| output_counts.get(name) == Some(&1));
    reserved.extend(output_counts.into_keys());
    let mut binder = Binder {
        bindings,
        reserved,
        hidden: vec![],
        added: vec![],
        referenced: HashSet::new(),
    };
    binder.bind(&mut query.having_expr_opt.as_mut().unwrap().expr)?;
    if !binder.hidden.is_empty() && query.group_by_exprs_opt.is_none() && !has_selected_aggregate {
        return Err(ParseError::InvalidArguments(
            "HAVING aggregates require an explicit GROUP BY or an aggregate in SELECT".into(),
        ));
    }
    let ast::SelectClause::SelectExpressions(selected) = &mut query.select_clause else {
        unreachable!()
    };
    selected.extend(binder.added);
    Ok(HavingRewrite {
        hidden: binder.hidden,
        referenced: binder.referenced,
    })
}

pub(super) fn visible_output(group: &types::Node, hidden: &[String]) -> Vec<types::Named> {
    let types::Node::GroupBy(fields, aggregates, _) = group else {
        unreachable!("expected grouped HAVING input")
    };
    let names = fields
        .iter()
        .map(PathExpr::unwrap_last)
        .chain(aggregates.iter().enumerate().map(|(index, aggregate)| {
            aggregate
                .name_opt
                .clone()
                .unwrap_or_else(|| format!("_{}", fields.len() + index + 1))
        }));
    names
        .filter(|name| !hidden.contains(name))
        .map(|name| {
            types::Named::Expression(
                types::Expression::Variable(PathExpr::new(vec![PathSegment::AttrName(name.clone())])),
                Some(name),
            )
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn select(query: &str) -> ast::SelectStatement {
        let (rest, query) = crate::syntax::parser::query(query).unwrap();
        assert!(rest.trim().is_empty());
        let ast::Query::Select(query) = crate::syntax::desugar::desugar_query(query) else {
            unreachable!()
        };
        *query
    }

    #[test]
    fn repeated_and_selected_having_aggregates_share_one_state() {
        let mut query =
            select("select k, sum(x) as total from it group by k having SUM(x) > 0 and count(*) > 1 and count(*) < 4");
        let rewritten = rewrite(&mut query).unwrap();
        assert_eq!(rewritten.hidden, ["__logq_having_1"]);
        assert!(rewritten.referenced.contains("total"));
        let ast::SelectClause::SelectExpressions(selected) = query.select_clause else {
            unreachable!()
        };
        assert_eq!(
            selected.len(),
            3,
            "selected SUM and repeated COUNT must not add duplicate states"
        );
    }

    #[test]
    fn inner_subquery_aggregates_remain_in_their_own_scope() {
        let mut query = select("select count(*) as n from it having count(*) > (select count(*) from it)");
        let original = query.having_expr_opt.as_ref().unwrap().expr.clone();
        let rewritten = rewrite(&mut query).unwrap();
        assert!(rewritten.hidden.is_empty());
        let Expression::BinaryOperator(_, _, original_right) = original else {
            unreachable!()
        };
        let Expression::BinaryOperator(_, _, right) = &query.having_expr_opt.unwrap().expr else {
            unreachable!()
        };
        assert_eq!(right, &original_right);
    }
}
