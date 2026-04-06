use hashbrown::HashSet;

use crate::execution::log_schema::LogSchema;
use crate::execution::types::{Aggregate, Expression, Formula, Named, NamedAggregate, Node};
use crate::syntax::ast::PathSegment;

/// Walk an Expression, collecting all field names it references.
fn collect_expr_fields(expr: &Expression, out: &mut HashSet<String>) {
    match expr {
        Expression::Variable(path_expr) => {
            // Extract attribute name from the first segment of the path
            if let Some(PathSegment::AttrName(name)) = path_expr.path_segments.first() {
                out.insert(name.clone());
            }
            // Wildcards in path mean we need all fields
            for seg in &path_expr.path_segments {
                if matches!(seg, PathSegment::Wildcard | PathSegment::WildcardAttr) {
                    out.insert("*".to_string());
                }
            }
        }
        Expression::Constant(_) => {}
        Expression::Function(_, args) => {
            for arg in args {
                collect_named_fields(arg, out);
            }
        }
        Expression::Branch(branches, else_expr) => {
            for (formula, then_expr) in branches {
                collect_formula_fields(formula, out);
                collect_expr_fields(then_expr, out);
            }
            if let Some(expr) = else_expr {
                collect_expr_fields(expr, out);
            }
        }
        Expression::Cast(inner, _) => {
            collect_expr_fields(inner, out);
        }
        Expression::Logic(formula) => {
            collect_formula_fields(formula, out);
        }
        Expression::Subquery(_) => {
            // Conservative: subquery may reference any field
            out.insert("*".to_string());
        }
    }
}

/// Walk a Formula, collecting all field names it references.
fn collect_formula_fields(formula: &Formula, out: &mut HashSet<String>) {
    match formula {
        Formula::Constant(_) => {}
        Formula::Predicate(_, left, right) => {
            collect_expr_fields(left, out);
            collect_expr_fields(right, out);
        }
        Formula::And(left, right) | Formula::Or(left, right) => {
            collect_formula_fields(left, out);
            collect_formula_fields(right, out);
        }
        Formula::Not(inner) => {
            collect_formula_fields(inner, out);
        }
        Formula::IsNull(e)
        | Formula::IsNotNull(e)
        | Formula::IsMissing(e)
        | Formula::IsNotMissing(e)
        | Formula::ExpressionPredicate(e) => {
            collect_expr_fields(e, out);
        }
        Formula::Like(left, right) | Formula::NotLike(left, right) => {
            collect_expr_fields(left, out);
            collect_expr_fields(right, out);
        }
        Formula::In(expr, list) | Formula::NotIn(expr, list) => {
            collect_expr_fields(expr, out);
            for item in list {
                collect_expr_fields(item, out);
            }
        }
    }
}

/// Handle Named::Expression and Named::Star.
fn collect_named_fields(named: &Named, out: &mut HashSet<String>) {
    match named {
        Named::Expression(expr, _) => {
            collect_expr_fields(expr, out);
        }
        Named::Star => {
            out.insert("*".to_string());
        }
    }
}

/// Walk a plan tree Node, collecting all field names referenced.
fn collect_node_fields(node: &Node, out: &mut HashSet<String>) {
    match node {
        Node::Map(named_list, source) => {
            for named in named_list {
                collect_named_fields(named, out);
            }
            collect_node_fields(source, out);
        }
        Node::Filter(source, formula) => {
            collect_formula_fields(formula, out);
            collect_node_fields(source, out);
        }
        Node::GroupBy(keys, aggregates, source) => {
            // Collect key path names
            for key_path in keys {
                if let Some(PathSegment::AttrName(name)) = key_path.path_segments.first() {
                    out.insert(name.clone());
                }
            }
            // Collect aggregate field references
            collect_aggregate_fields(aggregates, out);
            collect_node_fields(source, out);
        }
        Node::Limit(_, source) | Node::Distinct(source) => {
            collect_node_fields(source, out);
        }
        Node::OrderBy(columns, _, source) => {
            for col_path in columns {
                if let Some(PathSegment::AttrName(name)) = col_path.path_segments.first() {
                    out.insert(name.clone());
                }
            }
            collect_node_fields(source, out);
        }
        Node::DataSource(_, _) => {
            // Base case — no further recursion
        }
        Node::CrossJoin(left, right) | Node::Union(left, right) => {
            collect_node_fields(left, out);
            collect_node_fields(right, out);
        }
        Node::LeftJoin(left, right, condition) => {
            collect_node_fields(left, out);
            collect_node_fields(right, out);
            collect_formula_fields(condition, out);
        }
        Node::Intersect(left, right, _) | Node::Except(left, right, _) => {
            collect_node_fields(left, out);
            collect_node_fields(right, out);
        }
    }
}

/// Collect field names from aggregates in a GroupBy node.
fn collect_aggregate_fields(aggregates: &[NamedAggregate], out: &mut HashSet<String>) {
    for na in aggregates {
        match &na.aggregate {
            Aggregate::Avg(_, named)
            | Aggregate::Count(_, named)
            | Aggregate::First(_, named)
            | Aggregate::Last(_, named)
            | Aggregate::Max(_, named)
            | Aggregate::Min(_, named)
            | Aggregate::Sum(_, named)
            | Aggregate::ApproxCountDistinct(_, named)
            | Aggregate::GroupAs(_, named) => {
                collect_named_fields(named, out);
            }
            Aggregate::PercentileDisc(_, col_name)
            | Aggregate::ApproxPercentile(_, col_name) => {
                out.insert(col_name.clone());
            }
        }
    }
}

/// Convert a set of field names to sorted, deduplicated schema indices.
/// If `"*"` is present, returns all field indices.
fn resolve_field_names(names: &HashSet<String>, schema: &LogSchema) -> Vec<usize> {
    if names.contains("*") {
        return (0..schema.field_count()).collect();
    }
    let mut indices: Vec<usize> = names
        .iter()
        .filter_map(|name| schema.field_index(name))
        .collect();
    indices.sort_unstable();
    indices.dedup();
    indices
}

/// Extract field indices referenced by a Formula (for filter pushdown).
pub(crate) fn extract_fields_from_formula(formula: &Formula, schema: &LogSchema) -> Vec<usize> {
    let mut names = HashSet::new();
    collect_formula_fields(formula, &mut names);
    resolve_field_names(&names, schema)
}

/// Extract all field indices referenced anywhere in a plan tree.
/// Main entry point for field dependency analysis.
pub(crate) fn extract_required_fields(node: &Node, schema: &LogSchema) -> Vec<usize> {
    let mut names = HashSet::new();
    collect_node_fields(node, &mut names);
    resolve_field_names(&names, schema)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::types::{DataSource, Value};
    use crate::execution::types::{Expression, Formula, Named, Node, Relation};
    use crate::syntax::ast::{PathExpr, PathSegment};
    use std::path::PathBuf;

    /// Helper: build a Variable expression for a single field name.
    fn var(name: &str) -> Expression {
        Expression::Variable(PathExpr::new(vec![PathSegment::AttrName(name.to_string())]))
    }

    /// Helper: build a string constant.
    fn str_const(s: &str) -> Expression {
        Expression::Constant(Value::String(s.to_string()))
    }

    fn elb_schema() -> LogSchema {
        LogSchema::from_format("elb")
    }

    #[test]
    fn test_simple_filter_extracts_one_field() {
        let formula = Formula::Predicate(
            Relation::Equal,
            Box::new(var("elb_status_code")),
            Box::new(str_const("200")),
        );
        let schema = elb_schema();
        let fields = extract_fields_from_formula(&formula, &schema);
        assert_eq!(fields.len(), 1);
        assert_eq!(fields[0], schema.field_index("elb_status_code").unwrap());
    }

    #[test]
    fn test_and_filter_extracts_both_fields() {
        let formula = Formula::And(
            Box::new(Formula::Predicate(
                Relation::Equal,
                Box::new(var("elb_status_code")),
                Box::new(str_const("200")),
            )),
            Box::new(Formula::Predicate(
                Relation::MoreThan,
                Box::new(var("request_processing_time")),
                Box::new(str_const("0.5")),
            )),
        );
        let schema = elb_schema();
        let fields = extract_fields_from_formula(&formula, &schema);
        assert_eq!(fields.len(), 2);
        assert!(fields.contains(&schema.field_index("elb_status_code").unwrap()));
        assert!(fields.contains(&schema.field_index("request_processing_time").unwrap()));
    }

    #[test]
    fn test_node_star_returns_all_fields() {
        let schema = elb_schema();
        let node = Node::Map(
            vec![Named::Star],
            Box::new(Node::DataSource(
                DataSource::File(PathBuf::from("/dev/null"), "elb".to_string(), "log".to_string()),
                vec![],
            )),
        );
        let fields = extract_required_fields(&node, &schema);
        assert_eq!(fields.len(), schema.field_count());
    }

    #[test]
    fn test_node_select_specific_fields() {
        let schema = elb_schema();
        let node = Node::Map(
            vec![Named::Expression(var("elb_status_code"), None)],
            Box::new(Node::DataSource(
                DataSource::File(PathBuf::from("/dev/null"), "elb".to_string(), "log".to_string()),
                vec![],
            )),
        );
        let fields = extract_required_fields(&node, &schema);
        assert_eq!(fields.len(), 1);
        assert_eq!(fields[0], schema.field_index("elb_status_code").unwrap());
    }

    #[test]
    fn test_filter_plus_projection() {
        let schema = elb_schema();
        let node = Node::Map(
            vec![Named::Expression(var("timestamp"), None)],
            Box::new(Node::Filter(
                Box::new(Node::DataSource(
                    DataSource::File(PathBuf::from("/dev/null"), "elb".to_string(), "log".to_string()),
                    vec![],
                )),
                Box::new(Formula::Predicate(
                    Relation::Equal,
                    Box::new(var("elb_status_code")),
                    Box::new(str_const("200")),
                )),
            )),
        );
        let fields = extract_required_fields(&node, &schema);
        assert_eq!(fields.len(), 2);
        assert!(fields.contains(&schema.field_index("timestamp").unwrap()));
        assert!(fields.contains(&schema.field_index("elb_status_code").unwrap()));
    }

    #[test]
    fn test_expression_predicate_recurses() {
        let formula = Formula::ExpressionPredicate(Box::new(var("elb_status_code")));
        let schema = elb_schema();
        let fields = extract_fields_from_formula(&formula, &schema);
        assert_eq!(fields.len(), 1);
        assert_eq!(fields[0], schema.field_index("elb_status_code").unwrap());
    }

    #[test]
    fn test_like_extracts_field() {
        let formula = Formula::Like(
            Box::new(var("request")),
            Box::new(str_const("%GET%")),
        );
        let schema = elb_schema();
        let fields = extract_fields_from_formula(&formula, &schema);
        assert_eq!(fields.len(), 1);
        assert_eq!(fields[0], schema.field_index("request").unwrap());
    }

    #[test]
    fn test_unknown_field_ignored() {
        let formula = Formula::Predicate(
            Relation::Equal,
            Box::new(var("nonexistent_field")),
            Box::new(str_const("200")),
        );
        let schema = elb_schema();
        let fields = extract_fields_from_formula(&formula, &schema);
        assert_eq!(fields.len(), 0);
    }
}
