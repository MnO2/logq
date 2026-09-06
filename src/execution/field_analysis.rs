use hashbrown::HashSet;

use crate::execution::log_schema::LogSchema;
use crate::execution::types::{Aggregate, Expression, Formula, Named, NamedAggregate, Node};
use crate::syntax::ast::{PathExpr, PathSegment};
use std::collections::HashMap;

/// A required value either escapes as a whole or is only traversed through
/// named object attributes. Arrays and wildcard paths retain complete roots.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum JsonFieldProjection {
    All,
    Object(HashMap<String, JsonFieldProjection>),
}

impl JsonFieldProjection {
    fn insert(&mut self, path: &[PathSegment]) {
        if path.is_empty() {
            *self = Self::All;
        } else if let Self::Object(fields) = self {
            let PathSegment::AttrName(name) = &path[0] else {
                *self = Self::All;
                return;
            };
            fields
                .entry(name.clone())
                .or_insert_with(|| Self::Object(HashMap::new()))
                .insert(&path[1..]);
        }
    }

    pub(crate) fn children(&self) -> Option<&HashMap<String, Self>> {
        match self {
            Self::All => None,
            Self::Object(fields) => Some(fields),
        }
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct JsonProjection {
    names: Vec<String>,
    fields: HashMap<String, JsonFieldProjection>,
    all: bool,
}

impl JsonProjection {
    pub(crate) fn from_roots(roots: Vec<String>) -> Self {
        let mut projection = Self::default();
        for root in roots {
            projection.root(&root);
        }
        projection
    }

    pub(crate) fn names(&self) -> &[String] {
        &self.names
    }

    pub(crate) fn fields(&self) -> &HashMap<String, JsonFieldProjection> {
        &self.fields
    }

    pub(crate) fn retain_roots(&mut self, mut retain: impl FnMut(&str) -> bool) {
        self.fields.retain(|name, _| retain(name));
        self.names.retain(|name| self.fields.contains_key(name));
    }
}

/// Reuse the same plan walk for fixed-schema roots and JSON object paths.
trait FieldCollector {
    fn path(&mut self, path: &PathExpr);
    fn root(&mut self, name: &str);
    fn all(&mut self);
}

impl FieldCollector for HashSet<String> {
    fn path(&mut self, path: &PathExpr) {
        if let Some(PathSegment::AttrName(name) | PathSegment::ArrayIndex(name, _)) = path.path_segments.first() {
            self.root(name);
        }
        if path
            .path_segments
            .iter()
            .any(|segment| matches!(segment, PathSegment::Wildcard | PathSegment::WildcardAttr))
        {
            self.all();
        }
    }

    fn root(&mut self, name: &str) {
        self.insert(name.to_owned());
    }

    fn all(&mut self) {
        self.root("*");
    }
}

impl FieldCollector for JsonProjection {
    fn path(&mut self, path: &PathExpr) {
        if path
            .path_segments
            .iter()
            .any(|segment| matches!(segment, PathSegment::Wildcard | PathSegment::WildcardAttr))
        {
            self.all();
            return;
        }
        let Some(PathSegment::AttrName(name) | PathSegment::ArrayIndex(name, _)) = path.path_segments.first() else {
            self.all();
            return;
        };
        // Indexed access has separate runtime semantics; keep its entire root.
        if path
            .path_segments
            .iter()
            .any(|segment| matches!(segment, PathSegment::ArrayIndex(..)))
        {
            self.root(name);
            return;
        }
        if !self.fields.contains_key(name) {
            self.names.push(name.clone());
        }
        self.fields
            .entry(name.clone())
            .or_insert_with(|| JsonFieldProjection::Object(HashMap::new()))
            .insert(&path.path_segments[1..]);
    }

    fn root(&mut self, name: &str) {
        if self.fields.insert(name.to_owned(), JsonFieldProjection::All).is_none() {
            self.names.push(name.to_owned());
        }
    }

    fn all(&mut self) {
        self.all = true;
    }
}

/// Walk an Expression, collecting all field names it references.
fn collect_expr_fields(expr: &Expression, out: &mut impl FieldCollector) {
    match expr {
        Expression::Variable(path_expr) => {
            out.path(path_expr);
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
            out.all();
        }
    }
}

/// Walk a Formula, collecting all field names it references.
fn collect_formula_fields(formula: &Formula, out: &mut impl FieldCollector) {
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
fn collect_named_fields(named: &Named, out: &mut impl FieldCollector) {
    match named {
        Named::Expression(expr, _) => {
            collect_expr_fields(expr, out);
        }
        Named::Star => {
            out.all();
        }
    }
}

/// Walk a plan tree Node, collecting all field names referenced.
fn collect_node_fields(node: &Node, out: &mut impl FieldCollector) {
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
                out.path(key_path);
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
                out.path(col_path);
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
        Node::HashJoin {
            left,
            right,
            equi_keys,
            residual,
            ..
        } => {
            collect_node_fields(left, out);
            collect_node_fields(right, out);
            for (lk, rk) in equi_keys {
                out.path(lk);
                out.path(rk);
            }
            if let Some(r) = residual {
                collect_formula_fields(r, out);
            }
        }
    }
}

/// Collect field names from aggregates in a GroupBy node.
fn collect_aggregate_fields(aggregates: &[NamedAggregate], out: &mut impl FieldCollector) {
    for na in aggregates {
        match &na.aggregate {
            Aggregate::Count(_, Named::Star) => {}
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
            Aggregate::PercentileDisc(_, col_name) | Aggregate::ApproxPercentile(_, col_name) => {
                out.root(col_name);
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
    let mut indices: Vec<usize> = names.iter().filter_map(|name| schema.field_index(name)).collect();
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

/// Required JSON root fields. None preserves complete objects for wildcard,
/// bound-table, join and subquery shapes; Some(empty) is a validation-only scan.
pub(crate) fn extract_required_json_fields(node: &Node) -> Option<JsonProjection> {
    fn has_output_projection(node: &Node) -> bool {
        match node {
            Node::Map(..) | Node::GroupBy(..) => true,
            Node::Filter(source, _) | Node::Limit(_, source) | Node::OrderBy(_, _, source) | Node::Distinct(source) => {
                has_output_projection(source)
            }
            _ => false,
        }
    }
    if !has_output_projection(node) {
        return None;
    }

    fn has_scoped_source(node: &Node) -> bool {
        match node {
            Node::DataSource(_, bindings) => !bindings.is_empty(),
            Node::Map(_, source)
            | Node::Filter(source, _)
            | Node::GroupBy(_, _, source)
            | Node::Limit(_, source)
            | Node::OrderBy(_, _, source)
            | Node::Distinct(source) => has_scoped_source(source),
            _ => true,
        }
    }
    if has_scoped_source(node) {
        return None;
    }
    let mut fields = JsonProjection::default();
    collect_node_fields(node, &mut fields);
    if fields.all {
        return None;
    }
    fields.names.sort();
    Some(fields)
}

/// Root-only view for pipeline eligibility and fixed-schema diagnostics.
pub(crate) fn extract_required_root_names(node: &Node) -> Option<Vec<String>> {
    extract_required_json_fields(node).map(|fields| fields.names)
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
        Expression::Constant(Value::String(s.to_string().into()))
    }

    fn elb_schema() -> LogSchema {
        LogSchema::from_format("elb")
    }

    #[test]
    fn bare_json_source_requires_complete_records() {
        let source = Node::DataSource(
            DataSource::File(PathBuf::from("input.jsonl"), "jsonl".into(), "it".into()),
            vec![],
        );
        assert_eq!(extract_required_root_names(&source), None);
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
        let formula = Formula::Like(Box::new(var("request")), Box::new(str_const("%GET%")));
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

    #[test]
    fn json_projection_array_paths_retain_complete_roots_and_wildcards_disable_pruning() {
        let source = || Box::new(Node::DataSource(DataSource::Stdin("jsonl".into(), "it".into()), vec![]));
        for segments in [
            vec![
                PathSegment::ArrayIndex("items".into(), 0),
                PathSegment::AttrName("v".into()),
            ],
            vec![
                PathSegment::AttrName("items".into()),
                PathSegment::ArrayIndex("nested".into(), 0),
                PathSegment::AttrName("v".into()),
            ],
        ] {
            let node = Node::Map(
                vec![Named::Expression(Expression::Variable(PathExpr::new(segments)), None)],
                source(),
            );
            let fields = extract_required_json_fields(&node).unwrap();
            assert_eq!(fields.fields().get("items"), Some(&JsonFieldProjection::All));
        }
        for segment in [PathSegment::Wildcard, PathSegment::WildcardAttr] {
            let node = Node::Map(
                vec![Named::Expression(
                    Expression::Variable(PathExpr::new(vec![PathSegment::AttrName("items".into()), segment])),
                    None,
                )],
                source(),
            );
            assert!(extract_required_json_fields(&node).is_none());
        }
    }
}
