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

/// Evaluate a physical Formula against a ColumnBatch, returning a Bitmap
/// of rows where the predicate is true.
pub(crate) fn evaluate_batch_predicate(
    formula: &Formula,
    batch: &ColumnBatch,
    variables: &Variables,
    registry: &Arc<FunctionRegistry>,
) -> StreamResult<Bitmap> {
    match formula {
        Formula::Constant(val) => {
            if *val {
                Ok(Bitmap::all_set(batch.len))
            } else {
                Ok(Bitmap::all_unset(batch.len))
            }
        }
        Formula::Predicate(relation, left, right) => {
            evaluate_comparison(relation, left, right, batch, variables, registry)
        }
        Formula::And(left, right) => {
            let left_bm = evaluate_batch_predicate(left, batch, variables, registry)?;
            // Short-circuit: if left eliminated all rows, skip right entirely
            if left_bm.count_ones() == 0 {
                return Ok(left_bm);
            }
            let right_bm = evaluate_batch_predicate_masked(right, batch, variables, registry, Some(&left_bm))?;
            Ok(left_bm.and(&right_bm))
        }
        Formula::Or(left, right) => {
            let left_bm = evaluate_batch_predicate(left, batch, variables, registry)?;
            let right_bm = evaluate_batch_predicate(right, batch, variables, registry)?;
            Ok(left_bm.or(&right_bm))
        }
        Formula::Not(inner) => {
            let bm = evaluate_batch_predicate(inner, batch, variables, registry)?;
            Ok(bm.not(batch.len))
        }
        Formula::IsNull(expr) => evaluate_is_null(expr, batch),
        Formula::IsNotNull(expr) => {
            let bm = evaluate_is_null(expr, batch)?;
            Ok(bm.not(batch.len))
        }
        Formula::IsMissing(expr) => evaluate_is_missing(expr, batch),
        Formula::IsNotMissing(expr) => {
            let bm = evaluate_is_missing(expr, batch)?;
            Ok(bm.not(batch.len))
        }
        // Fallback for Like, NotLike, In, NotIn, ExpressionPredicate
        _ => evaluate_scalar_fallback(formula, batch, variables, registry),
    }
}

/// Like `evaluate_batch_predicate` but with an optional pre-filter mask.
/// When a mask is provided, scalar fallback paths skip rows where the mask
/// bit is unset, avoiding expensive per-row evaluation for already-eliminated rows.
fn evaluate_batch_predicate_masked(
    formula: &Formula,
    batch: &ColumnBatch,
    variables: &Variables,
    registry: &Arc<FunctionRegistry>,
    pre_mask: Option<&Bitmap>,
) -> StreamResult<Bitmap> {
    match formula {
        // For SIMD-accelerated paths (column vs constant), the kernel is cheap
        // enough that masking overhead isn't worthwhile -- evaluate all rows.
        // Only the scalar fallback benefits from masking.
        Formula::Predicate(relation, left, right) => {
            evaluate_comparison(relation, left, right, batch, variables, registry)
        }
        // Recurse with mask for nested AND
        Formula::And(left, right) => {
            let left_bm = evaluate_batch_predicate_masked(left, batch, variables, registry, pre_mask)?;
            if left_bm.count_ones() == 0 {
                return Ok(left_bm);
            }
            // Combine pre_mask with left result for right side
            let combined = match pre_mask {
                Some(mask) => mask.and(&left_bm),
                None => left_bm.clone(),
            };
            let right_bm = evaluate_batch_predicate_masked(right, batch, variables, registry, Some(&combined))?;
            Ok(left_bm.and(&right_bm))
        }
        // For these patterns, delegate to the regular evaluator
        // (they use SIMD kernels or simple bitmap ops, not scalar fallback)
        Formula::Constant(_)
        | Formula::Or(_, _)
        | Formula::Not(_)
        | Formula::IsNull(_)
        | Formula::IsNotNull(_)
        | Formula::IsMissing(_)
        | Formula::IsNotMissing(_) => {
            evaluate_batch_predicate(formula, batch, variables, registry)
        }
        // Fallback patterns (Like, In, ExpressionPredicate, etc.) benefit from masking
        _ => evaluate_scalar_fallback_masked(formula, batch, variables, registry, pre_mask),
    }
}

fn evaluate_comparison(
    relation: &Relation,
    left: &Expression,
    right: &Expression,
    batch: &ColumnBatch,
    variables: &Variables,
    registry: &Arc<FunctionRegistry>,
) -> StreamResult<Bitmap> {
    // Fast path: Variable op Constant
    if let (Expression::Variable(path), Expression::Constant(const_val)) = (left, right) {
        if let Some(col_name) = single_attr_name(path) {
            if let Some(col_idx) = batch.names.iter().position(|n| n == col_name) {
                return evaluate_column_vs_constant(
                    &batch.columns[col_idx],
                    relation,
                    const_val,
                    batch.len,
                );
            }
        }
    }
    // Fast path: Constant op Variable (flip relation)
    if let (Expression::Constant(const_val), Expression::Variable(path)) = (left, right) {
        if let Some(col_name) = single_attr_name(path) {
            if let Some(col_idx) = batch.names.iter().position(|n| n == col_name) {
                let flipped = flip_relation(relation);
                return evaluate_column_vs_constant(
                    &batch.columns[col_idx],
                    &flipped,
                    const_val,
                    batch.len,
                );
            }
        }
    }
    // Fallback to scalar
    evaluate_scalar_fallback(
        &Formula::Predicate(
            relation.clone(),
            Box::new(left.clone()),
            Box::new(right.clone()),
        ),
        batch,
        variables,
        registry,
    )
}

fn evaluate_column_vs_constant(
    col: &TypedColumn,
    relation: &Relation,
    constant: &Value,
    len: usize,
) -> StreamResult<Bitmap> {
    match (col, constant) {
        // String equality -- use filter cache for dedup
        (TypedColumn::Utf8 { data, offsets, null, missing }, Value::String(needle))
            if matches!(relation, Relation::Equal) =>
        {
            let needle_bytes = needle.as_bytes();
            let bm =
                evaluate_cached_two_pass(data, offsets, &|field: &[u8]| field == needle_bytes, len);
            let valid = null.and(missing);
            Ok(bm.and(&valid))
        }
        // String not-equal
        (TypedColumn::Utf8 { data, offsets, null, missing }, Value::String(needle))
            if matches!(relation, Relation::NotEqual) =>
        {
            let needle_bytes = needle.as_bytes();
            let bm =
                evaluate_cached_two_pass(data, offsets, &|field: &[u8]| field != needle_bytes, len);
            let valid = null.and(missing);
            Ok(bm.and(&valid))
        }
        // String ordering comparisons -- use filter cache with byte comparison
        (TypedColumn::Utf8 { data, offsets, null, missing }, Value::String(needle))
            if matches!(
                relation,
                Relation::MoreThan
                    | Relation::LessThan
                    | Relation::GreaterEqual
                    | Relation::LessEqual
            ) =>
        {
            let needle_bytes = needle.as_bytes();
            let cmp_fn: Box<dyn Fn(&[u8]) -> bool> = match relation {
                Relation::MoreThan => Box::new(|field: &[u8]| field > needle_bytes),
                Relation::LessThan => Box::new(|field: &[u8]| field < needle_bytes),
                Relation::GreaterEqual => Box::new(|field: &[u8]| field >= needle_bytes),
                Relation::LessEqual => Box::new(|field: &[u8]| field <= needle_bytes),
                _ => unreachable!(),
            };
            let bm = evaluate_cached_two_pass(data, offsets, &*cmp_fn, len);
            let valid = null.and(missing);
            Ok(bm.and(&valid))
        }
        // DictUtf8: compare needle against dictionary entries, then broadcast via codes
        (TypedColumn::DictUtf8 { dict_data, dict_offsets, codes, null, missing }, Value::String(needle)) => {
            let needle_bytes = needle.as_bytes();
            let dict_size = dict_offsets.len() - 1;
            // Build a match table: for each dictionary code, does it match?
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
            // Broadcast: look up each row's code in the match table
            let mut result_bytes = vec![0u8; len];
            kernels::dict_broadcast(codes, &match_table, &mut result_bytes);
            let bm = Bitmap::pack_from_bytes(&result_bytes);
            let valid = null.and(missing);
            Ok(bm.and(&valid))
        }
        // Int32 comparisons -- use SIMD kernels
        (TypedColumn::Int32 { data, null, missing }, Value::Int(threshold)) => {
            let threshold = *threshold;
            let mut result_bytes = vec![0u8; len];
            match relation {
                Relation::Equal => kernels::filter_eq_i32(data, threshold, &mut result_bytes),
                Relation::MoreThan => kernels::filter_gt_i32(data, threshold, &mut result_bytes),
                Relation::LessThan => kernels::filter_lt_i32(data, threshold, &mut result_bytes),
                Relation::GreaterEqual => {
                    kernels::filter_ge_i32(data, threshold, &mut result_bytes)
                }
                Relation::LessEqual => kernels::filter_le_i32(data, threshold, &mut result_bytes),
                Relation::NotEqual => kernels::filter_ne_i32(data, threshold, &mut result_bytes),
            }
            let bm = Bitmap::pack_from_bytes(&result_bytes);
            let valid = null.and(missing);
            Ok(bm.and(&valid))
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
            let valid = null.and(missing);
            Ok(bm.and(&valid))
        }
        // Fallback: row-by-row for Mixed columns and type mismatches
        _ => {
            let mut result = Bitmap::all_unset(len);
            for row in 0..len {
                let val = BatchToRowAdapter::extract_value(col, row);
                if matches!(val, Value::Null | Value::Missing) {
                    continue;
                }
                let matches = match relation {
                    Relation::Equal => val == *constant,
                    Relation::NotEqual => val != *constant,
                    Relation::MoreThan => compare_values_ord(&val, constant, |a, b| a > b),
                    Relation::LessThan => compare_values_ord(&val, constant, |a, b| a < b),
                    Relation::GreaterEqual => compare_values_ord(&val, constant, |a, b| a >= b),
                    Relation::LessEqual => compare_values_ord(&val, constant, |a, b| a <= b),
                };
                if matches {
                    result.set(row);
                }
            }
            Ok(result)
        }
    }
}

/// Helper for ordering comparisons on Value pairs in the general fallback path.
fn compare_values_ord(
    left: &Value,
    right: &Value,
    cmp: impl Fn(std::cmp::Ordering, std::cmp::Ordering) -> bool,
) -> bool {
    use ordered_float::OrderedFloat;
    use std::cmp::Ordering;
    match (left, right) {
        (Value::Int(l), Value::Int(r)) => cmp(l.cmp(r), Ordering::Equal),
        (Value::Float(l), Value::Float(r)) => cmp(l.cmp(r), Ordering::Equal),
        (Value::Int(l), Value::Float(r)) => {
            cmp(OrderedFloat::from(*l as f32).cmp(r), Ordering::Equal)
        }
        (Value::Float(l), Value::Int(r)) => {
            cmp(l.cmp(&OrderedFloat::from(*r as f32)), Ordering::Equal)
        }
        (Value::String(l), Value::String(r)) => cmp(l.cmp(r), Ordering::Equal),
        (Value::DateTime(l), Value::DateTime(r)) => cmp(l.cmp(r), Ordering::Equal),
        _ => false,
    }
}

fn evaluate_is_null(expr: &Expression, batch: &ColumnBatch) -> StreamResult<Bitmap> {
    if let Some(col_name) = expr_to_column_name(expr) {
        if let Some(col_idx) = batch.names.iter().position(|n| n == col_name) {
            let (null, missing) = get_null_missing_bitmaps(&batch.columns[col_idx]);
            // IS NULL: missing=1 AND null=0
            return Ok(missing.and(&null.not(batch.len)));
        }
    }
    Ok(Bitmap::all_unset(batch.len))
}

fn evaluate_is_missing(expr: &Expression, batch: &ColumnBatch) -> StreamResult<Bitmap> {
    if let Some(col_name) = expr_to_column_name(expr) {
        if let Some(col_idx) = batch.names.iter().position(|n| n == col_name) {
            let (_, missing) = get_null_missing_bitmaps(&batch.columns[col_idx]);
            // IS MISSING: missing=0
            return Ok(missing.not(batch.len));
        }
    }
    // Column not found -> all rows are "missing" for that column
    Ok(Bitmap::all_set(batch.len))
}

fn evaluate_scalar_fallback(
    formula: &Formula,
    batch: &ColumnBatch,
    variables: &Variables,
    registry: &Arc<FunctionRegistry>,
) -> StreamResult<Bitmap> {
    let mut result = Bitmap::all_unset(batch.len);
    for row in 0..batch.len {
        let mut row_vars = variables.clone();
        for (i, col) in batch.columns.iter().enumerate() {
            let val = BatchToRowAdapter::extract_value(col, row);
            row_vars.insert(batch.names[i].clone(), val);
        }
        match formula.evaluate(&row_vars, registry) {
            Ok(Some(true)) => result.set(row),
            _ => {}
        }
    }
    Ok(result)
}

fn evaluate_scalar_fallback_masked(
    formula: &Formula,
    batch: &ColumnBatch,
    variables: &Variables,
    registry: &Arc<FunctionRegistry>,
    pre_mask: Option<&Bitmap>,
) -> StreamResult<Bitmap> {
    let mut result = Bitmap::all_unset(batch.len);
    for row in 0..batch.len {
        // Skip rows already eliminated by a previous predicate
        if let Some(mask) = pre_mask {
            if !mask.is_set(row) {
                continue;
            }
        }
        let mut row_vars = variables.clone();
        for (i, col) in batch.columns.iter().enumerate() {
            let val = BatchToRowAdapter::extract_value(col, row);
            row_vars.insert(batch.names[i].clone(), val);
        }
        match formula.evaluate(&row_vars, registry) {
            Ok(Some(true)) => result.set(row),
            _ => {}
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
            Box::new(Expression::Variable(PathExpr::new(vec![
                PathSegment::AttrName("status".to_string()),
            ]))),
            Box::new(Expression::Constant(Value::String("200".to_string()))),
        );
        let registry = Arc::new(crate::functions::register_all().unwrap());
        let result =
            evaluate_batch_predicate(&formula, &batch, &Variables::new(), &registry).unwrap();
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
            Box::new(Expression::Variable(PathExpr::new(vec![
                PathSegment::AttrName("code".to_string()),
            ]))),
            Box::new(Expression::Constant(Value::Int(200))),
        );
        let registry = Arc::new(crate::functions::register_all().unwrap());
        let result =
            evaluate_batch_predicate(&formula, &batch, &Variables::new(), &registry).unwrap();
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
            Box::new(Expression::Variable(PathExpr::new(vec![
                PathSegment::AttrName("code".to_string()),
            ]))),
            Box::new(Expression::Constant(Value::Int(200))),
        );
        let f2 = Formula::Predicate(
            Relation::LessEqual,
            Box::new(Expression::Variable(PathExpr::new(vec![
                PathSegment::AttrName("code".to_string()),
            ]))),
            Box::new(Expression::Constant(Value::Int(300))),
        );
        let formula = Formula::And(Box::new(f1), Box::new(f2));
        let registry = Arc::new(crate::functions::register_all().unwrap());
        let result =
            evaluate_batch_predicate(&formula, &batch, &Variables::new(), &registry).unwrap();
        assert_eq!(result.count_ones(), 2); // 200, 300
    }

    #[test]
    fn test_evaluate_not() {
        let batch = make_int_batch(&[100, 200, 300]);
        let inner = Formula::Predicate(
            Relation::Equal,
            Box::new(Expression::Variable(PathExpr::new(vec![
                PathSegment::AttrName("code".to_string()),
            ]))),
            Box::new(Expression::Constant(Value::Int(200))),
        );
        let formula = Formula::Not(Box::new(inner));
        let registry = Arc::new(crate::functions::register_all().unwrap());
        let result =
            evaluate_batch_predicate(&formula, &batch, &Variables::new(), &registry).unwrap();
        assert_eq!(result.count_ones(), 2); // 100, 300
    }

    #[test]
    fn test_evaluate_constant_true() {
        let batch = make_int_batch(&[1, 2, 3]);
        let registry = Arc::new(crate::functions::register_all().unwrap());
        let result = evaluate_batch_predicate(
            &Formula::Constant(true),
            &batch,
            &Variables::new(),
            &registry,
        )
        .unwrap();
        assert_eq!(result.count_ones(), 3);
    }

    #[test]
    fn test_evaluate_constant_false() {
        let batch = make_int_batch(&[1, 2, 3]);
        let registry = Arc::new(crate::functions::register_all().unwrap());
        let result = evaluate_batch_predicate(
            &Formula::Constant(false),
            &batch,
            &Variables::new(),
            &registry,
        )
        .unwrap();
        assert_eq!(result.count_ones(), 0);
    }

    #[test]
    fn test_and_short_circuit_skips_right() {
        // Left side: code == 999 (no rows match)
        // Right side: anything (should never be evaluated)
        let batch = make_int_batch(&[100, 200, 300, 400]);
        let left = Formula::Predicate(
            Relation::Equal,
            Box::new(Expression::Variable(PathExpr::new(vec![
                PathSegment::AttrName("code".to_string()),
            ]))),
            Box::new(Expression::Constant(Value::Int(999))),
        );
        let right = Formula::Predicate(
            Relation::MoreThan,
            Box::new(Expression::Variable(PathExpr::new(vec![
                PathSegment::AttrName("code".to_string()),
            ]))),
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
            Box::new(Expression::Variable(PathExpr::new(vec![
                PathSegment::AttrName("status".to_string()),
            ]))),
            Box::new(Expression::Constant(Value::String("200".to_string()))),
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
            Box::new(Expression::Variable(PathExpr::new(vec![
                PathSegment::AttrName("status".to_string()),
            ]))),
            Box::new(Expression::Constant(Value::String("200".to_string()))),
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
            Box::new(Expression::Variable(PathExpr::new(vec![
                PathSegment::AttrName("status".to_string()),
            ]))),
            Box::new(Expression::Constant(Value::String("banana".to_string()))),
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
            Box::new(Expression::Variable(PathExpr::new(vec![
                PathSegment::AttrName("status".to_string()),
            ]))),
            Box::new(Expression::Constant(Value::String("999".to_string()))),
        );
        let registry = Arc::new(crate::functions::register_all().unwrap());
        let result = evaluate_batch_predicate(&formula, &batch, &Variables::new(), &registry).unwrap();
        assert_eq!(result.count_ones(), 0);
    }
}
