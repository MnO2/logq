// src/execution/batch_predicate.rs

use crate::common::types::{Value, Variables};
use crate::execution::batch::{BatchToRowAdapter, ColumnBatch, TypedColumn};
use crate::execution::types::{Expression, Formula, Relation, StreamResult};
use crate::functions::FunctionRegistry;
use crate::simd::bitmap::Bitmap;
use crate::simd::filter_cache::evaluate_cached_two_pass;
use crate::simd::kernels;
use crate::syntax::ast::{PathExpr, PathSegment};
use std::sync::Arc;

type BytePredicate<'a> = dyn Fn(&[u8]) -> bool + 'a;

/// TRUE and UNKNOWN must remain distinct until the complete WHERE formula is
/// evaluated. In particular, NOT UNKNOWN is UNKNOWN, and an UNKNOWN left side
/// of AND still evaluates its right side (including any evaluation error).
struct PredicateTruth {
    yes: Bitmap,
    unknown: Bitmap,
}

impl PredicateTruth {
    fn known(yes: Bitmap, len: usize) -> Self {
        Self {
            yes,
            unknown: Bitmap::all_unset(len),
        }
    }
}

pub(crate) fn evaluate_batch_predicate(
    formula: &Formula,
    batch: &ColumnBatch,
    variables: &Variables,
    registry: &Arc<FunctionRegistry>,
) -> StreamResult<Bitmap> {
    let active = batch.selection.to_bitmap(batch.len);
    Ok(evaluate_truth(formula, batch, variables, registry, &active)?.yes)
}

fn evaluate_truth(
    formula: &Formula,
    batch: &ColumnBatch,
    variables: &Variables,
    registry: &Arc<FunctionRegistry>,
    active: &Bitmap,
) -> StreamResult<PredicateTruth> {
    if !active.any() {
        return Ok(PredicateTruth::known(Bitmap::all_unset(batch.len), batch.len));
    }
    match formula {
        Formula::Constant(value) => Ok(PredicateTruth::known(
            if *value {
                active.clone()
            } else {
                Bitmap::all_unset(batch.len)
            },
            batch.len,
        )),
        Formula::And(left, right) => {
            let left = evaluate_truth(left, batch, variables, registry, active)?;
            let right_active = left.yes.or(&left.unknown);
            let right = evaluate_truth(right, batch, variables, registry, &right_active)?;
            let unknown = left
                .unknown
                .and(&right.yes.or(&right.unknown))
                .or(&left.yes.and(&right.unknown));
            Ok(PredicateTruth {
                yes: left.yes.and(&right.yes),
                unknown,
            })
        }
        Formula::Or(left, right) => {
            let left = evaluate_truth(left, batch, variables, registry, active)?;
            let right_active = active.and(&left.yes.not(batch.len));
            let right = evaluate_truth(right, batch, variables, registry, &right_active)?;
            let unknown = left.unknown.and(&right.yes.not(batch.len)).or(&right.unknown);
            Ok(PredicateTruth {
                yes: left.yes.or(&right.yes),
                unknown,
            })
        }
        Formula::Not(inner) => {
            let inner = evaluate_truth(inner, batch, variables, registry, active)?;
            Ok(PredicateTruth {
                yes: active.and(&inner.yes.or(&inner.unknown).not(batch.len)),
                unknown: inner.unknown,
            })
        }
        Formula::Predicate(relation, left, right) => {
            if let Expression::Variable(path) = left.as_ref() {
                if let (Some(index), Some(value)) = (
                    single_attr_name(path).and_then(|name| batch.names.iter().rposition(|n| n == name)),
                    invariant_value(right, batch, variables),
                ) {
                    return evaluate_column_truth(&batch.columns[index], relation, value, batch.len, active);
                }
            }
            if let Expression::Variable(path) = right.as_ref() {
                if let (Some(index), Some(value)) = (
                    single_attr_name(path).and_then(|name| batch.names.iter().rposition(|n| n == name)),
                    invariant_value(left, batch, variables),
                ) {
                    return evaluate_column_truth(
                        &batch.columns[index],
                        &flip_relation(relation),
                        value,
                        batch.len,
                        active,
                    );
                }
            }
            evaluate_scalar_truth(formula, batch, variables, registry, active)
        }
        Formula::IsNull(expr) | Formula::IsNotNull(expr) | Formula::IsMissing(expr) | Formula::IsNotMissing(expr) => {
            if let Some(index) = expr_to_column_name(expr).and_then(|name| batch.names.iter().rposition(|n| n == name))
            {
                let column = &batch.columns[index];
                // Mixed columns can contain actual NULL/MISSING values even when
                // older fixed-format producers mark their validity bits present.
                if !matches!(column, TypedColumn::Mixed { .. }) {
                    let (null, missing) = get_null_missing_bitmaps(column);
                    let yes = match formula {
                        Formula::IsNull(_) => missing.and(&null.not(batch.len)),
                        Formula::IsNotNull(_) => missing.and(&null.not(batch.len)).not(batch.len),
                        Formula::IsMissing(_) => missing.not(batch.len),
                        Formula::IsNotMissing(_) => missing.clone(),
                        _ => unreachable!(),
                    };
                    return Ok(PredicateTruth::known(yes.and(active), batch.len));
                }
            }
            evaluate_scalar_truth(formula, batch, variables, registry, active)
        }
        Formula::Like(expr, pattern) | Formula::NotLike(expr, pattern) => {
            if let Some(result) = try_dict_like_pushdown(
                expr,
                pattern,
                matches!(formula, Formula::NotLike(..)),
                batch,
                variables,
                registry,
                active,
            )? {
                return Ok(result);
            }
            evaluate_scalar_truth(formula, batch, variables, registry, active)
        }
        _ => evaluate_scalar_truth(formula, batch, variables, registry, active),
    }
}

// The planner hoists literals into the outer Variables scope. Borrow these
// values once per batch, while preserving row-column shadowing of scope names.
fn invariant_value<'a>(expression: &'a Expression, batch: &ColumnBatch, variables: &'a Variables) -> Option<&'a Value> {
    match expression {
        Expression::Constant(value) => Some(value),
        Expression::Variable(path) => {
            let name = single_attr_name(path)?;
            if batch.names.iter().any(|column| column == name) {
                None
            } else {
                Some(variables.get(name).unwrap_or(&Value::Missing))
            }
        }
        _ => None,
    }
}

fn evaluate_column_truth(
    column: &TypedColumn,
    relation: &Relation,
    constant: &Value,
    len: usize,
    active: &Bitmap,
) -> StreamResult<PredicateTruth> {
    if matches!(constant, Value::Null | Value::Missing) {
        return Ok(PredicateTruth {
            yes: Bitmap::all_unset(len),
            unknown: active.clone(),
        });
    }
    let use_kernel = match (column, constant) {
        (TypedColumn::Utf8 { .. } | TypedColumn::DictUtf8 { .. }, Value::String(_)) => true,
        (TypedColumn::Int32 { .. }, Value::Int(_)) => true,
        (TypedColumn::Float32 { data, .. }, Value::Float(value)) => {
            !value.is_nan() && !data.iter().any(|value| value.is_nan())
        }
        _ => false,
    };
    if use_kernel {
        let yes = evaluate_column_vs_constant(column, relation, constant, len)?.and(active);
        let unknown = column.validity_bitmap(len).not(len).and(active);
        return Ok(PredicateTruth { yes, unknown });
    }
    let mut result = PredicateTruth::known(Bitmap::all_unset(len), len);
    for row in 0..len {
        if !active.is_set(row) {
            continue;
        }
        let owned;
        let value = match column {
            TypedColumn::Mixed { data, null, missing } if null.is_set(row) && missing.is_set(row) => &data[row],
            _ => {
                owned = BatchToRowAdapter::extract_value(column, row);
                &owned
            }
        };
        match relation
            .compare_ref(value, constant)
            .map_err(crate::execution::types::EvaluateError::Expression)?
        {
            Some(true) => result.yes.set(row),
            None => result.unknown.set(row),
            Some(false) => {}
        }
    }
    Ok(result)
}

fn evaluate_column_vs_constant(
    col: &TypedColumn,
    relation: &Relation,
    constant: &Value,
    len: usize,
) -> StreamResult<Bitmap> {
    match (col, constant) {
        // String equality -- use filter cache for dedup
        (
            TypedColumn::Utf8 {
                data,
                offsets,
                null,
                missing,
            },
            Value::String(needle),
        ) if matches!(relation, Relation::Equal) => {
            let needle_bytes = needle.as_bytes();
            let bm = evaluate_cached_two_pass(data, offsets, &|field: &[u8]| field == needle_bytes, len);
            if col.all_present(len) {
                Ok(bm)
            } else {
                let valid = null.and(missing);
                Ok(bm.and(&valid))
            }
        }
        // String not-equal
        (
            TypedColumn::Utf8 {
                data,
                offsets,
                null,
                missing,
            },
            Value::String(needle),
        ) if matches!(relation, Relation::NotEqual) => {
            let needle_bytes = needle.as_bytes();
            let bm = evaluate_cached_two_pass(data, offsets, &|field: &[u8]| field != needle_bytes, len);
            if col.all_present(len) {
                Ok(bm)
            } else {
                let valid = null.and(missing);
                Ok(bm.and(&valid))
            }
        }
        // String ordering comparisons -- use filter cache with byte comparison
        (
            TypedColumn::Utf8 {
                data,
                offsets,
                null,
                missing,
            },
            Value::String(needle),
        ) if matches!(
            relation,
            Relation::MoreThan | Relation::LessThan | Relation::GreaterEqual | Relation::LessEqual
        ) =>
        {
            let needle_bytes = needle.as_bytes();
            let cmp_fn: Box<BytePredicate<'_>> = match relation {
                Relation::MoreThan => Box::new(|field: &[u8]| field > needle_bytes),
                Relation::LessThan => Box::new(|field: &[u8]| field < needle_bytes),
                Relation::GreaterEqual => Box::new(|field: &[u8]| field >= needle_bytes),
                Relation::LessEqual => Box::new(|field: &[u8]| field <= needle_bytes),
                _ => unreachable!(),
            };
            let bm = evaluate_cached_two_pass(data, offsets, &*cmp_fn, len);
            if col.all_present(len) {
                Ok(bm)
            } else {
                let valid = null.and(missing);
                Ok(bm.and(&valid))
            }
        }
        // DictUtf8: compare needle against dictionary entries, then broadcast via codes
        (
            TypedColumn::DictUtf8 {
                dict_data,
                dict_offsets,
                codes,
                null,
                missing,
            },
            Value::String(needle),
        ) => {
            let needle_bytes = needle.as_bytes();
            let dict_size = dict_offsets.len() - 1;
            let mut match_table = vec![0u8; dict_size];
            for c in 0..dict_size {
                let start = dict_offsets[c] as usize;
                let end = dict_offsets[c + 1] as usize;
                let entry = &dict_data[start..end];
                match_table[c] = match relation {
                    Relation::Equal => (entry == needle_bytes) as u8,
                    Relation::NotEqual => (entry != needle_bytes) as u8,
                    Relation::MoreThan => (entry > needle_bytes) as u8,
                    Relation::LessThan => (entry < needle_bytes) as u8,
                    Relation::GreaterEqual => (entry >= needle_bytes) as u8,
                    Relation::LessEqual => (entry <= needle_bytes) as u8,
                };
            }
            let mut result_bytes = vec![0u8; len];
            kernels::dict_broadcast(codes, &match_table, &mut result_bytes);
            let bm = Bitmap::pack_from_bytes(&result_bytes);
            if col.all_present(len) {
                Ok(bm)
            } else {
                let valid = null.and(missing);
                Ok(bm.and(&valid))
            }
        }
        // Int32 comparisons -- use SIMD kernels
        (TypedColumn::Int32 { data, null, missing }, Value::Int(threshold)) => {
            let threshold = *threshold;
            let mut result_bytes = vec![0u8; len];
            match relation {
                Relation::Equal => kernels::filter_eq_i32(data, threshold, &mut result_bytes),
                Relation::MoreThan => kernels::filter_gt_i32(data, threshold, &mut result_bytes),
                Relation::LessThan => kernels::filter_lt_i32(data, threshold, &mut result_bytes),
                Relation::GreaterEqual => kernels::filter_ge_i32(data, threshold, &mut result_bytes),
                Relation::LessEqual => kernels::filter_le_i32(data, threshold, &mut result_bytes),
                Relation::NotEqual => kernels::filter_ne_i32(data, threshold, &mut result_bytes),
            }
            let bm = Bitmap::pack_from_bytes(&result_bytes);
            if col.all_present(len) {
                Ok(bm)
            } else {
                let valid = null.and(missing);
                Ok(bm.and(&valid))
            }
        }
        // Float32 comparisons -- use SIMD kernels
        (TypedColumn::Float32 { data, null, missing }, Value::Float(threshold)) => {
            let t = threshold.into_inner();
            let mut result_bytes = vec![0u8; len];
            match relation {
                Relation::Equal => kernels::filter_eq_f32(data, t, &mut result_bytes),
                Relation::GreaterEqual => kernels::filter_ge_f32(data, t, &mut result_bytes),
                Relation::MoreThan => kernels::filter_gt_f32(data, t, &mut result_bytes),
                Relation::LessThan => kernels::filter_lt_f32(data, t, &mut result_bytes),
                Relation::LessEqual => kernels::filter_le_f32(data, t, &mut result_bytes),
                Relation::NotEqual => kernels::filter_ne_f32(data, t, &mut result_bytes),
            }
            let bm = Bitmap::pack_from_bytes(&result_bytes);
            if col.all_present(len) {
                Ok(bm)
            } else {
                let valid = null.and(missing);
                Ok(bm.and(&valid))
            }
        }
        _ => unreachable!("column kernels require matching primitive types"),
    }
}

/// Match only active strings, caching compiled patterns across batches and
/// evaluating each referenced dictionary entry at most once within a batch.
fn try_dict_like_pushdown(
    expr: &Expression,
    pattern_expr: &Expression,
    is_not_like: bool,
    batch: &ColumnBatch,
    variables: &Variables,
    registry: &Arc<FunctionRegistry>,
    active: &Bitmap,
) -> StreamResult<Option<PredicateTruth>> {
    let Some(col_idx) = expr_to_column_name(expr).and_then(|name| batch.names.iter().rposition(|n| n == name)) else {
        return Ok(None);
    };
    let col = &batch.columns[col_idx];
    if !matches!(col, TypedColumn::Utf8 { .. } | TypedColumn::DictUtf8 { .. }) {
        return Ok(None);
    }
    let pattern = match pattern_expr {
        Expression::Constant(value) => value.clone(),
        Expression::Variable(path) if matches!(path.path_segments.first(), Some(PathSegment::AttrName(name)) if !batch.names.contains(name)) => {
            pattern_expr.expression_value_impl(variables, None, registry)?
        }
        _ => return Ok(None),
    };
    if matches!(pattern, Value::Null | Value::Missing) {
        return Ok(Some(PredicateTruth {
            yes: Bitmap::all_unset(batch.len),
            unknown: active.clone(),
        }));
    }
    let Value::String(pattern) = pattern else {
        return Ok(None);
    };
    let valid = col.validity_bitmap(batch.len).and(active);
    let unknown = active.and(&valid.not(batch.len));
    if !valid.any() {
        return Ok(Some(PredicateTruth {
            yes: Bitmap::all_unset(batch.len),
            unknown,
        }));
    }
    let yes = crate::execution::types::with_like_regex(&pattern, |re| {
        let mut yes = Bitmap::all_unset(batch.len);
        match col {
            TypedColumn::DictUtf8 {
                dict_data,
                dict_offsets,
                codes,
                ..
            } => {
                let mut matches = vec![None; dict_offsets.len() - 1];
                for row in 0..batch.len {
                    if !valid.is_set(row) {
                        continue;
                    }
                    let code = codes[row] as usize;
                    let matched = *matches[code].get_or_insert_with(|| {
                        let bytes = &dict_data[dict_offsets[code] as usize..dict_offsets[code + 1] as usize];
                        re.is_match(&String::from_utf8_lossy(bytes)) != is_not_like
                    });
                    if matched {
                        yes.set(row);
                    }
                }
            }
            TypedColumn::Utf8 { data, offsets, .. } => {
                let mut matches = hashbrown::HashMap::with_capacity(32);
                for row in 0..batch.len {
                    if !valid.is_set(row) {
                        continue;
                    }
                    let bytes = &data[offsets[row] as usize..offsets[row + 1] as usize];
                    let matched = *matches
                        .entry(bytes)
                        .or_insert_with(|| re.is_match(&String::from_utf8_lossy(bytes)) != is_not_like);
                    if matched {
                        yes.set(row);
                    }
                }
            }
            _ => unreachable!(),
        }
        yes
    })?;
    Ok(Some(PredicateTruth { yes, unknown }))
}

fn evaluate_scalar_truth(
    formula: &Formula,
    batch: &ColumnBatch,
    variables: &Variables,
    registry: &Arc<FunctionRegistry>,
    active: &Bitmap,
) -> StreamResult<PredicateTruth> {
    let mut result = PredicateTruth::known(Bitmap::all_unset(batch.len), batch.len);
    // Reuse row-map nodes and keys; scope is borrowed rather than cloned per row.
    let mut row_vars = Variables::with_capacity(batch.columns.len());
    for row in 0..batch.len {
        if !active.is_set(row) {
            continue;
        }
        for (index, column) in batch.columns.iter().enumerate() {
            let value = BatchToRowAdapter::extract_value(column, row);
            if let Some(existing) = row_vars.get_mut(batch.names[index].as_str()) {
                *existing = value;
            } else {
                row_vars.insert(batch.names[index].clone(), value);
            }
        }
        match formula.evaluate_in_scope(&row_vars, variables, registry)? {
            Some(true) => result.yes.set(row),
            None => result.unknown.set(row),
            Some(false) => {}
        }
    }
    Ok(result)
}

fn single_attr_name(path: &PathExpr) -> Option<&str> {
    if path.path_segments.len() == 1 {
        if let PathSegment::AttrName(name) = &path.path_segments[0] {
            return Some(name);
        }
    }
    None
}

fn expr_to_column_name(expr: &Expression) -> Option<&str> {
    if let Expression::Variable(path) = expr {
        single_attr_name(path)
    } else {
        None
    }
}

fn flip_relation(r: &Relation) -> Relation {
    match r {
        Relation::Equal => Relation::Equal,
        Relation::NotEqual => Relation::NotEqual,
        Relation::MoreThan => Relation::LessThan,
        Relation::LessThan => Relation::MoreThan,
        Relation::GreaterEqual => Relation::LessEqual,
        Relation::LessEqual => Relation::GreaterEqual,
    }
}

fn get_null_missing_bitmaps(col: &TypedColumn) -> (&Bitmap, &Bitmap) {
    match col {
        TypedColumn::Int32 { null, missing, .. } => (null, missing),
        TypedColumn::Float32 { null, missing, .. } => (null, missing),
        TypedColumn::Boolean { null, missing, .. } => (null, missing),
        TypedColumn::Utf8 { null, missing, .. } => (null, missing),
        TypedColumn::DictUtf8 { null, missing, .. } => (null, missing),
        TypedColumn::DateTime { null, missing, .. } => (null, missing),
        TypedColumn::Mixed { null, missing, .. } => (null, missing),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::simd::padded_vec::PaddedVecBuilder;

    fn variable(name: &str) -> Expression {
        Expression::Variable(PathExpr::new(vec![PathSegment::AttrName(name.into())]))
    }

    fn mixed_batch(values: Vec<Value>) -> ColumnBatch {
        let len = values.len();
        ColumnBatch {
            columns: vec![TypedColumn::Mixed {
                data: values,
                null: Bitmap::all_set(len),
                missing: Bitmap::all_set(len),
            }],
            names: vec!["value".into()],
            selection: crate::simd::selection::SelectionVector::All,
            len,
        }
    }

    #[test]
    fn test_batch_predicate_nullable_logic_matches_scalar() {
        let batch = mixed_batch(vec![Value::Int(1), Value::Int(0), Value::Null, Value::Missing]);
        let eq = Formula::Predicate(
            Relation::Equal,
            Box::new(variable("value")),
            Box::new(Expression::Constant(Value::Int(1))),
        );
        let registry = Arc::new(crate::functions::register_all().unwrap());
        let formulas = [
            Formula::Not(Box::new(eq.clone())),
            Formula::Not(Box::new(Formula::And(
                Box::new(eq.clone()),
                Box::new(Formula::Constant(true)),
            ))),
            Formula::Not(Box::new(Formula::Or(
                Box::new(eq.clone()),
                Box::new(Formula::Constant(false)),
            ))),
            Formula::Or(
                Box::new(eq.clone()),
                Box::new(Formula::IsMissing(Box::new(variable("value")))),
            ),
            Formula::IsNull(Box::new(variable("value"))),
            Formula::IsMissing(Box::new(variable("value"))),
            Formula::Predicate(
                Relation::NotEqual,
                Box::new(variable("value")),
                Box::new(Expression::Constant(Value::Null)),
            ),
        ];
        for formula in formulas {
            let actual = evaluate_batch_predicate(&formula, &batch, &Variables::new(), &registry).unwrap();
            for row in 0..batch.len {
                let vars = [("value".into(), BatchToRowAdapter::extract_value(&batch.columns[0], row))]
                    .into_iter()
                    .collect();
                let expected = formula.evaluate(&vars, &registry).unwrap() == Some(true);
                assert_eq!(actual.is_set(row), expected, "{formula:?} row {row}");
            }
        }
    }

    #[test]
    fn test_batch_predicate_preserves_errors_and_short_circuiting() {
        let batch = mixed_batch(vec![Value::Null]);
        let registry = Arc::new(crate::functions::register_all().unwrap());
        let error = Formula::Like(
            Box::new(Expression::Constant(Value::Int(1))),
            Box::new(Expression::Constant(Value::String("%".into()))),
        );
        assert!(evaluate_batch_predicate(&error, &batch, &Variables::new(), &registry).is_err());
        let unknown = Formula::Predicate(
            Relation::Equal,
            Box::new(variable("value")),
            Box::new(Expression::Constant(Value::Int(1))),
        );
        assert!(
            evaluate_batch_predicate(
                &Formula::And(Box::new(unknown), Box::new(error.clone())),
                &batch,
                &Variables::new(),
                &registry
            )
            .is_err()
        );
        for formula in [
            Formula::And(Box::new(Formula::Constant(false)), Box::new(error.clone())),
            Formula::Or(Box::new(Formula::Constant(true)), Box::new(error)),
        ] {
            assert!(evaluate_batch_predicate(&formula, &batch, &Variables::new(), &registry).is_ok());
        }
        let ordering = Formula::Predicate(
            Relation::MoreThan,
            Box::new(variable("value")),
            Box::new(Expression::Constant(Value::Int(0))),
        );
        let batch = mixed_batch(vec![Value::String("wrong type".into())]);
        assert!(evaluate_batch_predicate(&ordering, &batch, &Variables::new(), &registry).is_err());
    }

    #[test]
    fn test_batch_predicate_masked_dictionary_like_with_scope_pattern() {
        let mut batch = make_dict_string_batch(&["Chrome", "Safari", "Chrome", "Chrome"]);
        if let TypedColumn::DictUtf8 { null, missing, .. } = &mut batch.columns[0] {
            null.unset(2);
            missing.unset(3);
        }
        let registry = Arc::new(crate::functions::register_all().unwrap());
        let variables = [("pattern".into(), Value::String("%Chrome%".into()))]
            .into_iter()
            .collect();
        let formula = Formula::And(
            Box::new(Formula::IsNotMissing(Box::new(variable("status")))),
            Box::new(Formula::Not(Box::new(Formula::Like(
                Box::new(variable("status")),
                Box::new(variable("pattern")),
            )))),
        );
        let actual = evaluate_batch_predicate(&formula, &batch, &variables, &registry).unwrap();
        assert_eq!(actual.count_ones(), 1);
        assert!(actual.is_set(1));
    }

    #[test]
    fn test_batch_predicate_selection_does_not_evaluate_inactive_errors() {
        let mut batch = mixed_batch(vec![Value::String("ok".into()), Value::Int(1)]);
        let mut selected = Bitmap::all_unset(2);
        selected.set(0);
        batch.selection = crate::simd::selection::SelectionVector::Bitmap(selected);
        let formula = Formula::Like(
            Box::new(variable("value")),
            Box::new(Expression::Constant(Value::String("%".into()))),
        );
        let registry = Arc::new(crate::functions::register_all().unwrap());
        let actual = evaluate_batch_predicate(&formula, &batch, &Variables::new(), &registry).unwrap();
        assert_eq!(actual.count_ones(), 1);
    }

    #[test]
    fn test_batch_predicate_complete_three_valued_truth_tables() {
        let states = [Value::Boolean(true), Value::Boolean(false), Value::Null, Value::Missing];
        let mut left = Vec::new();
        let mut right = Vec::new();
        for a in &states {
            for b in &states {
                left.push(a.clone());
                right.push(b.clone());
            }
        }
        let len = left.len();
        let batch = ColumnBatch {
            columns: [left, right]
                .into_iter()
                .map(|data| TypedColumn::Mixed {
                    data,
                    null: Bitmap::all_set(len),
                    missing: Bitmap::all_set(len),
                })
                .collect(),
            names: vec!["left".into(), "right".into()],
            selection: crate::simd::selection::SelectionVector::All,
            len,
        };
        let left = Formula::ExpressionPredicate(Box::new(variable("left")));
        let right = Formula::ExpressionPredicate(Box::new(variable("right")));
        let registry = Arc::new(crate::functions::register_all().unwrap());
        for formula in [
            Formula::And(Box::new(left.clone()), Box::new(right.clone())),
            Formula::Or(Box::new(left), Box::new(right)),
        ] {
            for formula in [formula.clone(), Formula::Not(Box::new(formula))] {
                let result = evaluate_batch_predicate(&formula, &batch, &Variables::new(), &registry).unwrap();
                for row in 0..len {
                    let vars = batch
                        .names
                        .iter()
                        .zip(&batch.columns)
                        .map(|(name, col)| (name.clone(), BatchToRowAdapter::extract_value(col, row)))
                        .collect();
                    assert_eq!(
                        result.is_set(row),
                        formula.evaluate(&vars, &registry).unwrap() == Some(true),
                        "{formula:?} row {row}"
                    );
                }
            }
        }
    }

    fn make_string_batch(values: &[&str]) -> ColumnBatch {
        let len = values.len();
        let mut data_builder = PaddedVecBuilder::<u8>::new();
        let mut offsets_builder = PaddedVecBuilder::<u32>::with_capacity(len + 1);
        offsets_builder.push(0);
        for s in values {
            data_builder.extend_from_slice(s.as_bytes());
            offsets_builder.push(data_builder.len() as u32);
        }
        let col = TypedColumn::Utf8 {
            data: data_builder.seal(),
            offsets: offsets_builder.seal(),
            null: Bitmap::all_set(len),
            missing: Bitmap::all_set(len),
        };
        ColumnBatch {
            columns: vec![col],
            names: vec!["status".to_string()],
            selection: crate::simd::selection::SelectionVector::All,
            len,
        }
    }

    fn make_int_batch(values: &[i32]) -> ColumnBatch {
        use crate::simd::padded_vec::PaddedVec;
        let len = values.len();
        let col = TypedColumn::Int32 {
            data: PaddedVec::from_vec(values.to_vec()),
            null: Bitmap::all_set(len),
            missing: Bitmap::all_set(len),
        };
        ColumnBatch {
            columns: vec![col],
            names: vec!["code".to_string()],
            selection: crate::simd::selection::SelectionVector::All,
            len,
        }
    }

    #[test]
    fn test_evaluate_string_equality() {
        let batch = make_string_batch(&["200", "404", "200", "500"]);
        let formula = Formula::Predicate(
            Relation::Equal,
            Box::new(Expression::Variable(PathExpr::new(vec![PathSegment::AttrName(
                "status".to_string(),
            )]))),
            Box::new(Expression::Constant(Value::String("200".to_string().into()))),
        );
        let registry = Arc::new(crate::functions::register_all().unwrap());
        let result = evaluate_batch_predicate(&formula, &batch, &Variables::new(), &registry).unwrap();
        assert_eq!(result.count_ones(), 2);
        assert!(result.is_set(0));
        assert!(!result.is_set(1));
        assert!(result.is_set(2));
        assert!(!result.is_set(3));
    }

    #[test]
    fn test_evaluate_int_greater_than() {
        let batch = make_int_batch(&[100, 200, 300, 400]);
        let formula = Formula::Predicate(
            Relation::MoreThan,
            Box::new(Expression::Variable(PathExpr::new(vec![PathSegment::AttrName(
                "code".to_string(),
            )]))),
            Box::new(Expression::Constant(Value::Int(200))),
        );
        let registry = Arc::new(crate::functions::register_all().unwrap());
        let result = evaluate_batch_predicate(&formula, &batch, &Variables::new(), &registry).unwrap();
        assert_eq!(result.count_ones(), 2); // 300, 400
        assert!(!result.is_set(0));
        assert!(!result.is_set(1));
        assert!(result.is_set(2));
        assert!(result.is_set(3));
    }

    #[test]
    fn test_evaluate_and() {
        let batch = make_int_batch(&[100, 200, 300, 400]);
        let f1 = Formula::Predicate(
            Relation::GreaterEqual,
            Box::new(Expression::Variable(PathExpr::new(vec![PathSegment::AttrName(
                "code".to_string(),
            )]))),
            Box::new(Expression::Constant(Value::Int(200))),
        );
        let f2 = Formula::Predicate(
            Relation::LessEqual,
            Box::new(Expression::Variable(PathExpr::new(vec![PathSegment::AttrName(
                "code".to_string(),
            )]))),
            Box::new(Expression::Constant(Value::Int(300))),
        );
        let formula = Formula::And(Box::new(f1), Box::new(f2));
        let registry = Arc::new(crate::functions::register_all().unwrap());
        let result = evaluate_batch_predicate(&formula, &batch, &Variables::new(), &registry).unwrap();
        assert_eq!(result.count_ones(), 2); // 200, 300
    }

    #[test]
    fn test_evaluate_not() {
        let batch = make_int_batch(&[100, 200, 300]);
        let inner = Formula::Predicate(
            Relation::Equal,
            Box::new(Expression::Variable(PathExpr::new(vec![PathSegment::AttrName(
                "code".to_string(),
            )]))),
            Box::new(Expression::Constant(Value::Int(200))),
        );
        let formula = Formula::Not(Box::new(inner));
        let registry = Arc::new(crate::functions::register_all().unwrap());
        let result = evaluate_batch_predicate(&formula, &batch, &Variables::new(), &registry).unwrap();
        assert_eq!(result.count_ones(), 2); // 100, 300
    }

    #[test]
    fn test_evaluate_constant_true() {
        let batch = make_int_batch(&[1, 2, 3]);
        let registry = Arc::new(crate::functions::register_all().unwrap());
        let result = evaluate_batch_predicate(&Formula::Constant(true), &batch, &Variables::new(), &registry).unwrap();
        assert_eq!(result.count_ones(), 3);
    }

    #[test]
    fn test_evaluate_constant_false() {
        let batch = make_int_batch(&[1, 2, 3]);
        let registry = Arc::new(crate::functions::register_all().unwrap());
        let result = evaluate_batch_predicate(&Formula::Constant(false), &batch, &Variables::new(), &registry).unwrap();
        assert_eq!(result.count_ones(), 0);
    }

    #[test]
    fn test_and_short_circuit_skips_right() {
        // Left side: code == 999 (no rows match)
        // Right side: anything (should never be evaluated)
        let batch = make_int_batch(&[100, 200, 300, 400]);
        let left = Formula::Predicate(
            Relation::Equal,
            Box::new(Expression::Variable(PathExpr::new(vec![PathSegment::AttrName(
                "code".to_string(),
            )]))),
            Box::new(Expression::Constant(Value::Int(999))),
        );
        let right = Formula::Predicate(
            Relation::MoreThan,
            Box::new(Expression::Variable(PathExpr::new(vec![PathSegment::AttrName(
                "code".to_string(),
            )]))),
            Box::new(Expression::Constant(Value::Int(0))),
        );
        let formula = Formula::And(Box::new(left), Box::new(right));
        let registry = Arc::new(crate::functions::register_all().unwrap());
        let result = evaluate_batch_predicate(&formula, &batch, &Variables::new(), &registry).unwrap();
        assert_eq!(result.count_ones(), 0);
    }

    fn make_dict_string_batch(values: &[&str]) -> ColumnBatch {
        use crate::simd::padded_vec::PaddedVec;
        let len = values.len();
        // Build dictionary from unique values
        let mut dict_map: hashbrown::HashMap<&str, u16> = hashbrown::HashMap::new();
        let mut dict_entries: Vec<&str> = Vec::new();
        let mut codes: Vec<u16> = Vec::with_capacity(len);
        for &v in values {
            let next = dict_map.len() as u16;
            let code = *dict_map.entry(v).or_insert_with(|| {
                dict_entries.push(v);
                next
            });
            codes.push(code);
        }
        let mut dict_data_builder = PaddedVecBuilder::<u8>::new();
        let mut dict_offsets_builder = PaddedVecBuilder::<u32>::with_capacity(dict_entries.len() + 1);
        dict_offsets_builder.push(0);
        for e in &dict_entries {
            dict_data_builder.extend_from_slice(e.as_bytes());
            dict_offsets_builder.push(dict_data_builder.len() as u32);
        }
        let col = TypedColumn::DictUtf8 {
            dict_data: dict_data_builder.seal(),
            dict_offsets: dict_offsets_builder.seal(),
            codes: PaddedVec::from_vec(codes),
            null: Bitmap::all_set(len),
            missing: Bitmap::all_set(len),
        };
        ColumnBatch {
            columns: vec![col],
            names: vec!["status".to_string()],
            selection: crate::simd::selection::SelectionVector::All,
            len,
        }
    }

    #[test]
    fn test_dict_utf8_equality() {
        let batch = make_dict_string_batch(&["200", "404", "200", "500", "200"]);
        let formula = Formula::Predicate(
            Relation::Equal,
            Box::new(Expression::Variable(PathExpr::new(vec![PathSegment::AttrName(
                "status".to_string(),
            )]))),
            Box::new(Expression::Constant(Value::String("200".to_string().into()))),
        );
        let registry = Arc::new(crate::functions::register_all().unwrap());
        let result = evaluate_batch_predicate(&formula, &batch, &Variables::new(), &registry).unwrap();
        assert_eq!(result.count_ones(), 3);
        assert!(result.is_set(0));
        assert!(!result.is_set(1));
        assert!(result.is_set(2));
        assert!(!result.is_set(3));
        assert!(result.is_set(4));
    }

    #[test]
    fn test_dict_utf8_not_equal() {
        let batch = make_dict_string_batch(&["200", "404", "200", "500"]);
        let formula = Formula::Predicate(
            Relation::NotEqual,
            Box::new(Expression::Variable(PathExpr::new(vec![PathSegment::AttrName(
                "status".to_string(),
            )]))),
            Box::new(Expression::Constant(Value::String("200".to_string().into()))),
        );
        let registry = Arc::new(crate::functions::register_all().unwrap());
        let result = evaluate_batch_predicate(&formula, &batch, &Variables::new(), &registry).unwrap();
        assert_eq!(result.count_ones(), 2);
        assert!(result.is_set(1)); // "404"
        assert!(result.is_set(3)); // "500"
    }

    #[test]
    fn test_dict_utf8_ordering() {
        let batch = make_dict_string_batch(&["apple", "banana", "cherry", "apple", "date"]);
        let formula = Formula::Predicate(
            Relation::MoreThan,
            Box::new(Expression::Variable(PathExpr::new(vec![PathSegment::AttrName(
                "status".to_string(),
            )]))),
            Box::new(Expression::Constant(Value::String("banana".to_string().into()))),
        );
        let registry = Arc::new(crate::functions::register_all().unwrap());
        let result = evaluate_batch_predicate(&formula, &batch, &Variables::new(), &registry).unwrap();
        assert_eq!(result.count_ones(), 2);
        assert!(result.is_set(2)); // "cherry"
        assert!(result.is_set(4)); // "date"
    }

    #[test]
    fn test_dict_utf8_no_match() {
        let batch = make_dict_string_batch(&["200", "404", "200"]);
        let formula = Formula::Predicate(
            Relation::Equal,
            Box::new(Expression::Variable(PathExpr::new(vec![PathSegment::AttrName(
                "status".to_string(),
            )]))),
            Box::new(Expression::Constant(Value::String("999".to_string().into()))),
        );
        let registry = Arc::new(crate::functions::register_all().unwrap());
        let result = evaluate_batch_predicate(&formula, &batch, &Variables::new(), &registry).unwrap();
        assert_eq!(result.count_ones(), 0);
    }
    #[test]
    fn test_batch_predicate_boolean_column_compares_scoped_literal() {
        let mut bits = Bitmap::all_unset(2);
        bits.set(0);
        let batch = ColumnBatch {
            columns: vec![TypedColumn::Boolean {
                data: bits,
                null: Bitmap::all_set(2),
                missing: Bitmap::all_set(2),
            }],
            names: vec!["keep".into()],
            selection: crate::simd::selection::SelectionVector::All,
            len: 2,
        };
        let mut scope = Variables::new();
        scope.insert("const_000000000".into(), Value::Boolean(true));
        let formula = Formula::Predicate(
            Relation::Equal,
            Box::new(variable("keep")),
            Box::new(variable("const_000000000")),
        );
        let actual = evaluate_batch_predicate(&formula, &batch, &scope, &Arc::new(FunctionRegistry::new())).unwrap();
        assert!(actual.is_set(0));
        assert!(!actual.is_set(1));
    }
    #[test]
    fn test_batch_predicate_scoped_numeric_literal_preserves_masks_and_shadowing() {
        use crate::simd::padded_vec::PaddedVec;
        let mut null = Bitmap::all_set(3);
        null.unset(2);
        let batch = ColumnBatch {
            columns: vec![TypedColumn::Int32 {
                data: PaddedVec::from_vec(vec![1, 3, 0]),
                null,
                missing: Bitmap::all_set(3),
            }],
            names: vec!["x".into()],
            selection: crate::simd::selection::SelectionVector::All,
            len: 3,
        };
        let mut scope = Variables::new();
        scope.insert("bound".into(), Value::Int(2));
        scope.insert("x".into(), Value::Int(99));
        let formula = Formula::Not(Box::new(Formula::Predicate(
            Relation::MoreThan,
            Box::new(variable("x")),
            Box::new(variable("bound")),
        )));
        let actual = evaluate_batch_predicate(&formula, &batch, &scope, &Arc::new(FunctionRegistry::new())).unwrap();
        assert!(actual.is_set(0));
        assert!(!actual.is_set(1));
        assert!(!actual.is_set(2));
    }
}
