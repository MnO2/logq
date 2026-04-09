// src/execution/batch_groupby.rs

use crate::common;
use crate::common::types::{Tuple, Value, Variables};
use crate::execution::batch::{
    BatchSchema, BatchStream, BatchToRowAdapter, ColumnBatch, TypedColumn,
};
use crate::execution::types::{
    Aggregate, Expression, Named, NamedAggregate, StreamResult, value_less_than,
};
use ordered_float::OrderedFloat;
use crate::functions::FunctionRegistry;
use crate::simd::bitmap::Bitmap;
use crate::simd::selection::SelectionVector;
use crate::syntax::ast::PathExpr;
use linked_hash_map::LinkedHashMap;
use std::collections::hash_set;
use std::sync::Arc;

/// Extract null and missing bitmaps from a TypedColumn.
fn get_null_missing(col: &TypedColumn) -> (&Bitmap, &Bitmap) {
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

/// Batch-native GroupBy operator that consumes batches from a child stream,
/// accumulates per-group aggregates using the existing `Aggregate` machinery,
/// then emits results as a single `ColumnBatch`.
pub(crate) struct BatchGroupByOperator {
    child: Box<dyn BatchStream>,
    group_keys: Vec<PathExpr>,
    aggregates: Vec<NamedAggregate>,
    variables: Variables,
    registry: Arc<FunctionRegistry>,
    consumed: bool,
    result_batch: Option<ColumnBatch>,
    schema: BatchSchema,
}

impl BatchGroupByOperator {
    pub fn new(
        child: Box<dyn BatchStream>,
        group_keys: Vec<PathExpr>,
        aggregates: Vec<NamedAggregate>,
        variables: Variables,
        registry: Arc<FunctionRegistry>,
    ) -> Self {
        // Placeholder schema — actual column names come from the emitted batch
        let schema = BatchSchema {
            names: vec![],
            types: vec![],
        };
        Self {
            child,
            group_keys,
            aggregates,
            variables,
            registry,
            consumed: false,
            result_batch: None,
            schema,
        }
    }

    /// Build a group key from the row variables for the configured group key columns.
    fn build_group_key(&self, row_vars: &Variables, scope: Option<&Variables>) -> Option<Tuple> {
        if self.group_keys.is_empty() {
            None
        } else {
            let key: Tuple = self
                .group_keys
                .iter()
                .map(|path| common::types::get_value_by_path_expr_scoped(path, 0, row_vars, scope))
                .collect();
            Some(key)
        }
    }

    /// Build column names for the output batch: group key names + aggregate names.
    fn build_column_names(&self) -> Vec<String> {
        let mut names = Vec::new();
        for (pos, k) in self.group_keys.iter().enumerate() {
            match k.path_segments.last() {
                Some(crate::syntax::ast::PathSegment::AttrName(s)) => {
                    names.push(s.clone());
                }
                _ => {
                    names.push(format!("_{}", pos + 1));
                }
            }
        }
        for (idx, named_agg) in self.aggregates.iter().enumerate() {
            if let Some(ref name) = named_agg.name_opt {
                names.push(name.clone());
            } else {
                names.push(format!("_{}", names.len() + 1));
            }
        }
        names
    }

    /// Check if all aggregates are simple enough for the column-direct fast path.
    /// Returns true when there are no group keys and every aggregate is either
    /// COUNT(*) or an aggregate over a plain column reference (no expression).
    fn is_ungrouped_simple(&self) -> bool {
        if !self.group_keys.is_empty() {
            return false;
        }
        self.aggregates.iter().all(|na| match &na.aggregate {
            Aggregate::Count(_, Named::Star) => true,
            Aggregate::Count(_, Named::Expression(Expression::Variable(p), _))
            | Aggregate::Sum(_, Named::Expression(Expression::Variable(p), _))
            | Aggregate::Avg(_, Named::Expression(Expression::Variable(p), _))
            | Aggregate::Min(_, Named::Expression(Expression::Variable(p), _))
            | Aggregate::Max(_, Named::Expression(Expression::Variable(p), _)) => {
                p.path_segments.len() == 1
                    && matches!(&p.path_segments[0], crate::syntax::ast::PathSegment::AttrName(_))
            }
            _ => false,
        })
    }

    /// Fast path for ungrouped aggregates: operate directly on typed columns
    /// instead of materializing each row into a LinkedHashMap.
    fn consume_ungrouped_fast(&mut self) -> StreamResult<Option<ColumnBatch>> {
        // Accumulators for each aggregate
        let n = self.aggregates.len();
        let mut count_star: Vec<i64> = vec![0; n];
        let mut count_col: Vec<i64> = vec![0; n];
        let mut sum_f64: Vec<f64> = vec![0.0; n];
        let mut min_val: Vec<Option<Value>> = vec![None; n];
        let mut max_val: Vec<Option<Value>> = vec![None; n];
        let mut is_count_star = vec![false; n];
        let mut col_names: Vec<Option<String>> = vec![None; n];

        // Pre-extract column names and aggregate kinds
        for (i, na) in self.aggregates.iter().enumerate() {
            match &na.aggregate {
                Aggregate::Count(_, Named::Star) => {
                    is_count_star[i] = true;
                }
                Aggregate::Count(_, Named::Expression(Expression::Variable(p), _))
                | Aggregate::Sum(_, Named::Expression(Expression::Variable(p), _))
                | Aggregate::Avg(_, Named::Expression(Expression::Variable(p), _))
                | Aggregate::Min(_, Named::Expression(Expression::Variable(p), _))
                | Aggregate::Max(_, Named::Expression(Expression::Variable(p), _)) => {
                    if let crate::syntax::ast::PathSegment::AttrName(name) = &p.path_segments[0] {
                        col_names[i] = Some(name.clone());
                    }
                }
                _ => {}
            }
        }

        while let Some(batch) = self.child.next_batch()? {
            let sel_bm = batch.selection.to_bitmap(batch.len);

            for (i, na) in self.aggregates.iter().enumerate() {
                if is_count_star[i] {
                    count_star[i] += sel_bm.count_ones() as i64;
                    continue;
                }

                let col_name = match &col_names[i] {
                    Some(name) => name,
                    None => continue,
                };
                let col_idx = match batch.names.iter().position(|n| n == col_name) {
                    Some(idx) => idx,
                    None => continue,
                };

                match &na.aggregate {
                    Aggregate::Count(_, _) => {
                        // COUNT(col): count non-null, non-missing active rows
                        let (null_bm, missing_bm) = get_null_missing(&batch.columns[col_idx]);
                        let valid = null_bm.and(missing_bm);
                        let active_valid = sel_bm.and(&valid);
                        count_col[i] += active_valid.count_ones() as i64;
                    }
                    Aggregate::Sum(_, _) | Aggregate::Avg(_, _) => {
                        match &batch.columns[col_idx] {
                            TypedColumn::Int32 { data, null, missing } => {
                                let valid = null.and(missing);
                                let active = sel_bm.and(&valid);
                                sum_f64[i] += crate::simd::kernels::sum_i32_selected(data, &active) as f64;
                                count_col[i] += active.count_ones() as i64;
                            }
                            TypedColumn::Float32 { data, null, missing } => {
                                let valid = null.and(missing);
                                let active = sel_bm.and(&valid);
                                let mask = active.unpack_to_bytes(batch.len);
                                let mut s = 0.0f64;
                                for j in 0..batch.len {
                                    s += (data[j] as f64) * (mask[j] as f64);
                                }
                                sum_f64[i] += s;
                                count_col[i] += active.count_ones() as i64;
                            }
                            TypedColumn::Mixed { data, null, missing } => {
                                let valid = null.and(missing);
                                let active = sel_bm.and(&valid);
                                for j in 0..batch.len {
                                    if active.is_set(j) {
                                        match &data[j] {
                                            Value::Int(v) => { sum_f64[i] += *v as f64; count_col[i] += 1; }
                                            Value::Float(v) => { sum_f64[i] += v.into_inner() as f64; count_col[i] += 1; }
                                            _ => {}
                                        }
                                    }
                                }
                            }
                            _ => {} // Utf8, Boolean, DateTime: sum not meaningful
                        }
                    }
                    Aggregate::Min(_, _) | Aggregate::Max(_, _) => {
                        let is_min = matches!(&na.aggregate, Aggregate::Min(_, _));
                        for j in 0..batch.len {
                            if !sel_bm.is_set(j) { continue; }
                            let val = BatchToRowAdapter::extract_value(&batch.columns[col_idx], j);
                            if matches!(val, Value::Null | Value::Missing) { continue; }
                            let slot = if is_min { &mut min_val[i] } else { &mut max_val[i] };
                            match slot {
                                None => { *slot = Some(val); }
                                Some(ref current) => {
                                    let replace = if is_min {
                                        value_less_than(&val, current)
                                    } else {
                                        value_less_than(current, &val)
                                    };
                                    if replace { *slot = Some(val); }
                                }
                            }
                        }
                    }
                    _ => {}
                }
            }
        }

        // Build result batch
        let column_names = self.build_column_names();
        let mut result_values: Vec<Value> = Vec::with_capacity(n);

        for (i, na) in self.aggregates.iter().enumerate() {
            let val = match &na.aggregate {
                Aggregate::Count(_, Named::Star) => Value::Int(count_star[i] as i32),
                Aggregate::Count(_, _) => Value::Int(count_col[i] as i32),
                Aggregate::Sum(_, _) => {
                    if count_col[i] == 0 {
                        Value::Null
                    } else {
                        Value::Float(OrderedFloat(sum_f64[i] as f32))
                    }
                }
                Aggregate::Avg(_, _) => {
                    if count_col[i] == 0 {
                        Value::Null
                    } else {
                        Value::Float(OrderedFloat((sum_f64[i] / count_col[i] as f64) as f32))
                    }
                }
                Aggregate::Min(_, _) => min_val[i].clone().unwrap_or(Value::Null),
                Aggregate::Max(_, _) => max_val[i].clone().unwrap_or(Value::Null),
                _ => Value::Null,
            };
            result_values.push(val);
        }

        let typed_columns: Vec<TypedColumn> = result_values
            .into_iter()
            .map(|v| TypedColumn::Mixed {
                data: vec![v],
                null: Bitmap::all_set(1),
                missing: Bitmap::all_set(1),
            })
            .collect();

        Ok(Some(ColumnBatch {
            columns: typed_columns,
            names: column_names,
            selection: SelectionVector::All,
            len: 1,
        }))
    }

    /// Check if grouped aggregation can use the columnar fast path.
    /// Requires all group keys to be plain column references and all aggregates
    /// to be COUNT(*) or simple column aggregates (COUNT/SUM/AVG/MIN/MAX).
    fn is_grouped_simple(&self) -> bool {
        if self.group_keys.is_empty() {
            return false;
        }
        let keys_ok = self.group_keys.iter().all(|k| {
            k.path_segments.len() == 1
                && matches!(&k.path_segments[0], crate::syntax::ast::PathSegment::AttrName(_))
        });
        if !keys_ok {
            return false;
        }
        self.aggregates.iter().all(|na| match &na.aggregate {
            Aggregate::Count(_, Named::Star) => true,
            Aggregate::Count(_, Named::Expression(Expression::Variable(p), _))
            | Aggregate::Sum(_, Named::Expression(Expression::Variable(p), _))
            | Aggregate::Avg(_, Named::Expression(Expression::Variable(p), _))
            | Aggregate::Min(_, Named::Expression(Expression::Variable(p), _))
            | Aggregate::Max(_, Named::Expression(Expression::Variable(p), _)) => {
                p.path_segments.len() == 1
                    && matches!(&p.path_segments[0], crate::syntax::ast::PathSegment::AttrName(_))
            }
            _ => false,
        })
    }

    /// Extract the string value at row `row` from a string column (Utf8 or DictUtf8).
    fn extract_string_key(col: &TypedColumn, row: usize) -> Vec<u8> {
        match col {
            TypedColumn::Utf8 { data, offsets, .. } => {
                let start = offsets[row] as usize;
                let end = offsets[row + 1] as usize;
                data[start..end].to_vec()
            }
            TypedColumn::DictUtf8 { dict_data, dict_offsets, codes, .. } => {
                let code = codes[row] as usize;
                let start = dict_offsets[code] as usize;
                let end = dict_offsets[code + 1] as usize;
                dict_data[start..end].to_vec()
            }
            _ => Vec::new(),
        }
    }

    /// Columnar fast path for grouped aggregation.
    /// Avoids materializing rows into LinkedHashMap by extracting group keys
    /// and aggregate values directly from typed columns.
    fn consume_grouped_fast(&mut self) -> StreamResult<Option<ColumnBatch>> {
        use hashbrown::HashMap as HBHashMap;

        let num_aggs = self.aggregates.len();
        let num_keys = self.group_keys.len();

        // Extract key column names
        let key_col_names: Vec<String> = self.group_keys.iter().map(|k| {
            if let crate::syntax::ast::PathSegment::AttrName(name) = &k.path_segments[0] {
                name.clone()
            } else {
                unreachable!()
            }
        }).collect();

        // Extract aggregate column names
        let mut agg_col_names: Vec<Option<String>> = vec![None; num_aggs];
        let mut is_count_star = vec![false; num_aggs];
        for (i, na) in self.aggregates.iter().enumerate() {
            match &na.aggregate {
                Aggregate::Count(_, Named::Star) => { is_count_star[i] = true; }
                Aggregate::Count(_, Named::Expression(Expression::Variable(p), _))
                | Aggregate::Sum(_, Named::Expression(Expression::Variable(p), _))
                | Aggregate::Avg(_, Named::Expression(Expression::Variable(p), _))
                | Aggregate::Min(_, Named::Expression(Expression::Variable(p), _))
                | Aggregate::Max(_, Named::Expression(Expression::Variable(p), _)) => {
                    if let crate::syntax::ast::PathSegment::AttrName(name) = &p.path_segments[0] {
                        agg_col_names[i] = Some(name.clone());
                    }
                }
                _ => {}
            }
        }

        // Per-group accumulators: group_index → accumulators
        // group_key_values[group_idx] = Vec<Value> of key column values
        let mut group_map: HBHashMap<Vec<u8>, usize> = HBHashMap::new();
        let mut group_key_values: Vec<Vec<Value>> = Vec::new();
        let mut count_star_acc: Vec<i64> = Vec::new();
        let mut count_col_acc: Vec<Vec<i64>> = vec![Vec::new(); num_aggs];
        let mut sum_acc: Vec<Vec<f64>> = vec![Vec::new(); num_aggs];
        let mut min_acc: Vec<Vec<Option<Value>>> = vec![Vec::new(); num_aggs];
        let mut max_acc: Vec<Vec<Option<Value>>> = vec![Vec::new(); num_aggs];

        while let Some(batch) = self.child.next_batch()? {
            let sel_bm = batch.selection.to_bitmap(batch.len);

            // Resolve key and aggregate column indices in this batch
            let key_col_indices: Vec<Option<usize>> = key_col_names.iter()
                .map(|name| batch.names.iter().position(|n| n == name))
                .collect();
            let agg_col_indices: Vec<Option<usize>> = agg_col_names.iter()
                .map(|name_opt| name_opt.as_ref().and_then(|name| batch.names.iter().position(|n| n == name)))
                .collect();

            for row in 0..batch.len {
                if !sel_bm.is_set(row) {
                    continue;
                }

                // Build a composite key as concatenated bytes with length prefixes
                let mut key_bytes = Vec::with_capacity(64);
                let mut key_values = Vec::with_capacity(num_keys);
                for (k, &col_idx_opt) in key_col_indices.iter().enumerate() {
                    let col_idx = match col_idx_opt {
                        Some(i) => i,
                        None => {
                            key_bytes.extend_from_slice(&0u32.to_le_bytes());
                            key_values.push(Value::Missing);
                            continue;
                        }
                    };
                    let col = &batch.columns[col_idx];
                    match col {
                        TypedColumn::Int32 { data, .. } => {
                            key_bytes.push(1); // type tag
                            key_bytes.extend_from_slice(&data[row].to_le_bytes());
                            key_values.push(Value::Int(data[row]));
                        }
                        TypedColumn::Utf8 { data, offsets, .. } => {
                            let start = offsets[row] as usize;
                            let end = offsets[row + 1] as usize;
                            let s = &data[start..end];
                            key_bytes.push(2); // type tag
                            key_bytes.extend_from_slice(&(s.len() as u32).to_le_bytes());
                            key_bytes.extend_from_slice(s);
                            key_values.push(Value::String(
                                String::from_utf8_lossy(s).into_owned().into()
                            ));
                        }
                        TypedColumn::DictUtf8 { dict_data, dict_offsets, codes, .. } => {
                            let code = codes[row] as usize;
                            let start = dict_offsets[code] as usize;
                            let end = dict_offsets[code + 1] as usize;
                            let s = &dict_data[start..end];
                            key_bytes.push(2);
                            key_bytes.extend_from_slice(&(s.len() as u32).to_le_bytes());
                            key_bytes.extend_from_slice(s);
                            key_values.push(Value::String(
                                String::from_utf8_lossy(s).into_owned().into()
                            ));
                        }
                        TypedColumn::Float32 { data, .. } => {
                            key_bytes.push(3);
                            key_bytes.extend_from_slice(&data[row].to_bits().to_le_bytes());
                            key_values.push(Value::Float(OrderedFloat(data[row])));
                        }
                        _ => {
                            let val = BatchToRowAdapter::extract_value(col, row);
                            key_bytes.push(0);
                            key_bytes.extend_from_slice(&format!("{:?}", val).as_bytes());
                            key_values.push(val);
                        }
                    }
                }

                // Look up or create group
                let num_groups = group_map.len();
                let group_idx = *group_map.entry(key_bytes).or_insert_with(|| {
                    group_key_values.push(key_values.clone());
                    count_star_acc.push(0);
                    for i in 0..num_aggs {
                        count_col_acc[i].push(0);
                        sum_acc[i].push(0.0);
                        min_acc[i].push(None);
                        max_acc[i].push(None);
                    }
                    num_groups
                });

                // Accumulate
                count_star_acc[group_idx] += 1;

                for (i, na) in self.aggregates.iter().enumerate() {
                    if is_count_star[i] {
                        continue;
                    }
                    let col_idx = match agg_col_indices[i] {
                        Some(idx) => idx,
                        None => continue,
                    };

                    match &na.aggregate {
                        Aggregate::Count(_, _) => {
                            let val = BatchToRowAdapter::extract_value(&batch.columns[col_idx], row);
                            if !matches!(val, Value::Null | Value::Missing) {
                                count_col_acc[i][group_idx] += 1;
                            }
                        }
                        Aggregate::Sum(_, _) | Aggregate::Avg(_, _) => {
                            match &batch.columns[col_idx] {
                                TypedColumn::Int32 { data, null, missing, .. } => {
                                    if null.is_set(row) && missing.is_set(row) {
                                        sum_acc[i][group_idx] += data[row] as f64;
                                        count_col_acc[i][group_idx] += 1;
                                    }
                                }
                                TypedColumn::Float32 { data, null, missing, .. } => {
                                    if null.is_set(row) && missing.is_set(row) {
                                        sum_acc[i][group_idx] += data[row] as f64;
                                        count_col_acc[i][group_idx] += 1;
                                    }
                                }
                                _ => {
                                    let val = BatchToRowAdapter::extract_value(&batch.columns[col_idx], row);
                                    match &val {
                                        Value::Int(v) => { sum_acc[i][group_idx] += *v as f64; count_col_acc[i][group_idx] += 1; }
                                        Value::Float(v) => { sum_acc[i][group_idx] += v.into_inner() as f64; count_col_acc[i][group_idx] += 1; }
                                        _ => {}
                                    }
                                }
                            }
                        }
                        Aggregate::Min(_, _) | Aggregate::Max(_, _) => {
                            let is_min = matches!(&na.aggregate, Aggregate::Min(_, _));
                            let val = BatchToRowAdapter::extract_value(&batch.columns[col_idx], row);
                            if matches!(val, Value::Null | Value::Missing) { continue; }
                            let slot = if is_min { &mut min_acc[i][group_idx] } else { &mut max_acc[i][group_idx] };
                            match slot {
                                None => { *slot = Some(val); }
                                Some(ref current) => {
                                    let replace = if is_min {
                                        value_less_than(&val, current)
                                    } else {
                                        value_less_than(current, &val)
                                    };
                                    if replace { *slot = Some(val); }
                                }
                            }
                        }
                        _ => {}
                    }
                }
            }
        }

        if group_map.is_empty() {
            return Ok(None);
        }

        // Build result batch
        let column_names = self.build_column_names();
        let num_rows = group_map.len();
        let total_cols = num_keys + num_aggs;
        let mut columns: Vec<Vec<Value>> = vec![Vec::with_capacity(num_rows); total_cols];

        for group_idx in 0..num_rows {
            // Key columns
            for (k, val) in group_key_values[group_idx].iter().enumerate() {
                columns[k].push(val.clone());
            }

            // Aggregate columns
            for (i, na) in self.aggregates.iter().enumerate() {
                let val = match &na.aggregate {
                    Aggregate::Count(_, Named::Star) => Value::Int(count_star_acc[group_idx] as i32),
                    Aggregate::Count(_, _) => Value::Int(count_col_acc[i][group_idx] as i32),
                    Aggregate::Sum(_, _) => {
                        if count_col_acc[i][group_idx] == 0 {
                            Value::Null
                        } else {
                            Value::Float(OrderedFloat(sum_acc[i][group_idx] as f32))
                        }
                    }
                    Aggregate::Avg(_, _) => {
                        if count_col_acc[i][group_idx] == 0 {
                            Value::Null
                        } else {
                            Value::Float(OrderedFloat((sum_acc[i][group_idx] / count_col_acc[i][group_idx] as f64) as f32))
                        }
                    }
                    Aggregate::Min(_, _) => min_acc[i][group_idx].clone().unwrap_or(Value::Null),
                    Aggregate::Max(_, _) => max_acc[i][group_idx].clone().unwrap_or(Value::Null),
                    _ => Value::Null,
                };
                columns[num_keys + i].push(val);
            }
        }

        let typed_columns: Vec<TypedColumn> = columns
            .into_iter()
            .map(|data| TypedColumn::Mixed {
                data,
                null: Bitmap::all_set(num_rows),
                missing: Bitmap::all_set(num_rows),
            })
            .collect();

        Ok(Some(ColumnBatch {
            columns: typed_columns,
            names: column_names,
            selection: SelectionVector::All,
            len: num_rows,
        }))
    }

    /// Consume all child batches, feed rows into aggregates, then build the result batch.
    fn consume_and_build(&mut self) -> StreamResult<Option<ColumnBatch>> {
        // Fast path: ungrouped aggregates with simple column references
        if self.is_ungrouped_simple() {
            return self.consume_ungrouped_fast();
        }
        // Fast path: grouped aggregates with simple column references
        if self.is_grouped_simple() {
            return self.consume_grouped_fast();
        }

        let mut groups: hash_set::HashSet<Option<Tuple>> = hash_set::HashSet::new();

        // Phase 1: consume all batches and accumulate
        while let Some(batch) = self.child.next_batch()? {
            for row_idx in 0..batch.len {
                if !batch.selection.is_active(row_idx, batch.len) {
                    continue;
                }

                // Build row variables from batch columns
                let mut row_vars = LinkedHashMap::with_capacity(batch.columns.len());
                for (col_idx, col) in batch.columns.iter().enumerate() {
                    let value = BatchToRowAdapter::extract_value(col, row_idx);
                    row_vars.insert(batch.names[col_idx].clone(), value);
                }

                // Use scoped lookup instead of merge to avoid allocations
                let variables = &row_vars;
                let scope: Option<&common::types::Variables> = if self.variables.is_empty() { None } else { Some(&self.variables) };

                // Build group key
                let key = self.build_group_key(variables, scope);

                // Track seen keys
                if !groups.contains(&key) {
                    groups.insert(key.clone());
                }

                // Feed into each aggregate
                for named_agg in self.aggregates.iter_mut() {
                    match &mut named_agg.aggregate {
                        Aggregate::Count(ref mut inner, named) => {
                            match named {
                                Named::Star => {
                                    inner.add_row(&key)?;
                                }
                                Named::Expression(expr, _) => {
                                    let val = expr
                                        .expression_value_impl(variables, scope, &self.registry)
                                        .map_err(crate::execution::types::StreamError::Expression)?;
                                    inner.add_record(&key, &val)?;
                                }
                            }
                        }
                        Aggregate::Sum(ref mut inner, named) => {
                            match named {
                                Named::Expression(expr, _) => {
                                    let val = expr
                                        .expression_value_impl(variables, scope, &self.registry)
                                        .map_err(crate::execution::types::StreamError::Expression)?;
                                    inner.add_record(&key, &val)?;
                                }
                                Named::Star => {} // no-op
                            }
                        }
                        Aggregate::Avg(ref mut inner, named) => {
                            match named {
                                Named::Expression(expr, _) => {
                                    let val = expr
                                        .expression_value_impl(variables, scope, &self.registry)
                                        .map_err(crate::execution::types::StreamError::Expression)?;
                                    inner.add_record(&key, &val)?;
                                }
                                Named::Star => {} // no-op
                            }
                        }
                        Aggregate::Min(ref mut inner, named) => {
                            match named {
                                Named::Expression(expr, _) => {
                                    let val = expr
                                        .expression_value_impl(variables, scope, &self.registry)
                                        .map_err(crate::execution::types::StreamError::Expression)?;
                                    inner.add_record(&key, &val)?;
                                }
                                Named::Star => {} // no-op
                            }
                        }
                        Aggregate::Max(ref mut inner, named) => {
                            match named {
                                Named::Expression(expr, _) => {
                                    let val = expr
                                        .expression_value_impl(variables, scope, &self.registry)
                                        .map_err(crate::execution::types::StreamError::Expression)?;
                                    inner.add_record(&key, &val)?;
                                }
                                Named::Star => {} // no-op
                            }
                        }
                        Aggregate::First(ref mut inner, named) => {
                            match named {
                                Named::Expression(expr, _) => {
                                    let val = expr
                                        .expression_value_impl(variables, scope, &self.registry)
                                        .map_err(crate::execution::types::StreamError::Expression)?;
                                    inner.add_record(&key, &val)?;
                                }
                                Named::Star => {} // no-op
                            }
                        }
                        Aggregate::Last(ref mut inner, named) => {
                            match named {
                                Named::Expression(expr, _) => {
                                    let val = expr
                                        .expression_value_impl(variables, scope, &self.registry)
                                        .map_err(crate::execution::types::StreamError::Expression)?;
                                    inner.add_record(&key, &val)?;
                                }
                                Named::Star => {} // no-op
                            }
                        }
                        Aggregate::ApproxCountDistinct(ref mut inner, named) => {
                            match named {
                                Named::Expression(expr, _) => {
                                    let val = expr
                                        .expression_value_impl(variables, scope, &self.registry)
                                        .map_err(crate::execution::types::StreamError::Expression)?;
                                    inner.add_record(&key, &val)?;
                                }
                                Named::Star => {} // no-op
                            }
                        }
                        Aggregate::GroupAs(ref mut inner, named) => {
                            match named {
                                Named::Expression(_, _) => {
                                    let val =
                                        Value::Object(Box::new(row_vars.clone()));
                                    inner.add_record(&key, &val)?;
                                }
                                Named::Star => {} // no-op
                            }
                        }
                        Aggregate::PercentileDisc(ref mut inner, col_name) => {
                            let val = common::types::scoped_get(variables, scope, col_name)
                                .cloned()
                                .unwrap_or(Value::Missing);
                            inner.add_record(&key, &val)?;
                        }
                        Aggregate::ApproxPercentile(ref mut inner, col_name) => {
                            let val = common::types::scoped_get(variables, scope, col_name)
                                .cloned()
                                .unwrap_or(Value::Missing);
                            inner.add_record(&key, &val)?;
                        }
                    }
                }
            }
        }

        // Phase 2: emit results
        let column_names = self.build_column_names();
        let num_key_cols = self.group_keys.len();

        // Handle case: no data seen and no group keys -> emit one row with defaults
        // COUNT → 0, all others → Null
        if groups.is_empty() && self.group_keys.is_empty() {
            let n = 1usize;
            let mut columns = Vec::with_capacity(self.aggregates.len());
            for named_agg in self.aggregates.iter() {
                let val = match &named_agg.aggregate {
                    Aggregate::Count(_, _) => Value::Int(0),
                    _ => Value::Null,
                };
                columns.push(TypedColumn::Mixed {
                    data: vec![val],
                    null: Bitmap::all_set(n),
                    missing: Bitmap::all_set(n),
                });
            }
            return Ok(Some(ColumnBatch {
                columns,
                names: column_names,
                selection: SelectionVector::All,
                len: n,
            }));
        }

        if groups.is_empty() {
            return Ok(None);
        }

        let keys_vec: Vec<Option<Tuple>> = groups.into_iter().collect();
        let num_rows = keys_vec.len();
        let total_cols = num_key_cols + self.aggregates.len();
        let mut columns: Vec<Vec<Value>> = vec![Vec::with_capacity(num_rows); total_cols];

        for key in &keys_vec {
            // Fill group key columns
            if let Some(ref vals) = key {
                for (col_idx, v) in vals.iter().enumerate() {
                    columns[col_idx].push(v.clone());
                }
            }

            // Fill aggregate columns
            for (agg_idx, named_agg) in self.aggregates.iter_mut().enumerate() {
                let val = named_agg.aggregate.get_aggregated(key)?;
                columns[num_key_cols + agg_idx].push(val);
            }
        }

        // Convert to TypedColumn::Mixed
        let typed_columns: Vec<TypedColumn> = columns
            .into_iter()
            .map(|data| TypedColumn::Mixed {
                data,
                null: Bitmap::all_set(num_rows),
                missing: Bitmap::all_set(num_rows),
            })
            .collect();

        Ok(Some(ColumnBatch {
            columns: typed_columns,
            names: column_names,
            selection: SelectionVector::All,
            len: num_rows,
        }))
    }
}

impl BatchStream for BatchGroupByOperator {
    fn next_batch(&mut self) -> StreamResult<Option<ColumnBatch>> {
        if self.consumed {
            return Ok(self.result_batch.take());
        }
        self.consumed = true;
        let batch = self.consume_and_build()?;
        self.result_batch = batch;
        Ok(self.result_batch.take())
    }

    fn schema(&self) -> &BatchSchema {
        &self.schema
    }

    fn close(&self) {
        self.child.close();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::types::Value;
    use crate::execution::batch::{BatchSchema, BatchStream, ColumnBatch, ColumnType, TypedColumn};
    use crate::execution::types::{
        Aggregate, CountAggregate, Named, NamedAggregate, StreamResult,
    };
    use crate::functions::FunctionRegistry;
    use crate::simd::bitmap::Bitmap;
    use crate::simd::padded_vec::PaddedVecBuilder;
    use crate::simd::selection::SelectionVector;
    use crate::syntax::ast::{PathExpr, PathSegment};
    use linked_hash_map::LinkedHashMap;
    use std::sync::Arc;

    /// A single-batch test helper stream.
    struct OneBatch {
        batch: Option<ColumnBatch>,
        schema: BatchSchema,
    }

    impl BatchStream for OneBatch {
        fn next_batch(&mut self) -> StreamResult<Option<ColumnBatch>> {
            Ok(self.batch.take())
        }
        fn schema(&self) -> &BatchSchema {
            &self.schema
        }
        fn close(&self) {}
    }

    /// An empty test helper stream.
    struct EmptyStream {
        schema: BatchSchema,
    }

    impl BatchStream for EmptyStream {
        fn next_batch(&mut self) -> StreamResult<Option<ColumnBatch>> {
            Ok(None)
        }
        fn schema(&self) -> &BatchSchema {
            &self.schema
        }
        fn close(&self) {}
    }

    /// Build a Utf8 TypedColumn from a slice of string values.
    fn build_utf8_column(values: &[&str]) -> TypedColumn {
        let n = values.len();
        let mut data_builder = PaddedVecBuilder::<u8>::new();
        let mut offsets_builder = PaddedVecBuilder::<u32>::with_capacity(n + 1);
        offsets_builder.push(0);
        for s in values {
            data_builder.extend_from_slice(s.as_bytes());
            offsets_builder.push(data_builder.len() as u32);
        }
        TypedColumn::Utf8 {
            data: data_builder.seal(),
            offsets: offsets_builder.seal(),
            null: Bitmap::all_set(n),
            missing: Bitmap::all_set(n),
        }
    }

    #[test]
    fn test_batch_groupby_count_star() {
        // 4 rows: status = ["200", "200", "404", "200"]
        // Group by status, COUNT(*)
        // Expected: 2 groups, "200" -> 3, "404" -> 1
        let status_col = build_utf8_column(&["200", "200", "404", "200"]);
        let batch = ColumnBatch {
            columns: vec![status_col],
            names: vec!["status".to_string()],
            selection: SelectionVector::All,
            len: 4,
        };
        let schema = BatchSchema {
            names: vec!["status".to_string()],
            types: vec![ColumnType::Utf8],
        };
        let child = OneBatch {
            batch: Some(batch),
            schema,
        };

        let group_keys = vec![PathExpr::new(vec![PathSegment::AttrName(
            "status".to_string(),
        )])];

        let count_agg = NamedAggregate::new(
            Aggregate::Count(CountAggregate::new(), Named::Star),
            Some("cnt".to_string()),
        );

        let registry = Arc::new(FunctionRegistry::new());
        let variables = LinkedHashMap::new();

        let mut op = BatchGroupByOperator::new(
            Box::new(child),
            group_keys,
            vec![count_agg],
            variables,
            registry,
        );

        let result = op.next_batch().unwrap().unwrap();
        assert_eq!(result.len, 2, "should have 2 groups");
        assert_eq!(result.columns.len(), 2, "should have 2 columns (status, cnt)");

        // Collect the results into a map for order-independent checking
        let mut group_counts: std::collections::HashMap<String, i32> =
            std::collections::HashMap::new();
        for row in 0..result.len {
            let status_val = BatchToRowAdapter::extract_value(&result.columns[0], row);
            let count_val = BatchToRowAdapter::extract_value(&result.columns[1], row);
            let status_str = match status_val {
                Value::String(s) => s,
                other => panic!("expected String, got {:?}", other),
            };
            let count_int = match count_val {
                Value::Int(i) => i,
                other => panic!("expected Int, got {:?}", other),
            };
            group_counts.insert(status_str.to_string(), count_int);
        }

        assert_eq!(group_counts.get("200"), Some(&3));
        assert_eq!(group_counts.get("404"), Some(&1));

        // Second call should return None
        assert!(op.next_batch().unwrap().is_none());
    }

    #[test]
    fn test_batch_groupby_empty_no_keys_returns_count_zero() {
        // Empty input stream, no group keys, COUNT(*)
        // Should return 1 row with COUNT = 0
        let schema = BatchSchema {
            names: vec![],
            types: vec![],
        };
        let child = EmptyStream { schema };

        let count_agg = NamedAggregate::new(
            Aggregate::Count(CountAggregate::new(), Named::Star),
            Some("cnt".to_string()),
        );

        let registry = Arc::new(FunctionRegistry::new());
        let variables = LinkedHashMap::new();

        let mut op = BatchGroupByOperator::new(
            Box::new(child),
            vec![], // no group keys
            vec![count_agg],
            variables,
            registry,
        );

        let result = op.next_batch().unwrap().unwrap();
        assert_eq!(result.len, 1, "should have 1 row for empty-input aggregate");
        assert_eq!(result.columns.len(), 1, "should have 1 column (cnt)");

        let count_val = BatchToRowAdapter::extract_value(&result.columns[0], 0);
        assert_eq!(count_val, Value::Int(0), "COUNT(*) on empty input should be 0");

        // Second call should return None
        assert!(op.next_batch().unwrap().is_none());
    }

    #[test]
    fn test_batch_groupby_ungrouped_fast_count_star() {
        // Ungrouped COUNT(*) over Int32 data -- should use column-direct fast path
        use crate::simd::padded_vec::PaddedVec;
        let col = TypedColumn::Int32 {
            data: PaddedVec::from_vec(vec![10, 20, 30, 40]),
            null: Bitmap::all_set(4),
            missing: Bitmap::all_set(4),
        };
        let batch = ColumnBatch {
            columns: vec![col],
            names: vec!["x".to_string()],
            selection: SelectionVector::All,
            len: 4,
        };
        let schema = BatchSchema {
            names: vec!["x".to_string()],
            types: vec![ColumnType::Int32],
        };

        let count_agg = NamedAggregate::new(
            Aggregate::Count(crate::execution::types::CountAggregate::new(), Named::Star),
            Some("cnt".to_string()),
        );

        let registry = Arc::new(FunctionRegistry::new());
        let mut op = BatchGroupByOperator::new(
            Box::new(OneBatch { batch: Some(batch), schema }),
            vec![],
            vec![count_agg],
            LinkedHashMap::new(),
            registry,
        );

        let result = op.next_batch().unwrap().unwrap();
        assert_eq!(result.len, 1);
        let count_val = BatchToRowAdapter::extract_value(&result.columns[0], 0);
        assert_eq!(count_val, Value::Int(4));
    }

    #[test]
    fn test_batch_groupby_ungrouped_fast_sum() {
        use crate::simd::padded_vec::PaddedVec;
        use crate::execution::types::SumAggregate;

        let col = TypedColumn::Int32 {
            data: PaddedVec::from_vec(vec![10, 20, 30, 40]),
            null: Bitmap::all_set(4),
            missing: Bitmap::all_set(4),
        };
        let batch = ColumnBatch {
            columns: vec![col],
            names: vec!["x".to_string()],
            selection: SelectionVector::All,
            len: 4,
        };
        let schema = BatchSchema {
            names: vec!["x".to_string()],
            types: vec![ColumnType::Int32],
        };

        let sum_agg = NamedAggregate::new(
            Aggregate::Sum(
                SumAggregate::new(),
                Named::Expression(
                    Expression::Variable(PathExpr::new(vec![
                        crate::syntax::ast::PathSegment::AttrName("x".to_string()),
                    ])),
                    Some("x".to_string()),
                ),
            ),
            Some("total".to_string()),
        );

        let registry = Arc::new(FunctionRegistry::new());
        let mut op = BatchGroupByOperator::new(
            Box::new(OneBatch { batch: Some(batch), schema }),
            vec![],
            vec![sum_agg],
            LinkedHashMap::new(),
            registry,
        );

        let result = op.next_batch().unwrap().unwrap();
        assert_eq!(result.len, 1);
        let sum_val = BatchToRowAdapter::extract_value(&result.columns[0], 0);
        assert_eq!(sum_val, Value::Float(OrderedFloat(100.0f32)));
    }

    #[test]
    fn test_batch_groupby_ungrouped_fast_with_selection() {
        use crate::simd::padded_vec::PaddedVec;

        // 4 rows, but only 2 active
        let col = TypedColumn::Int32 {
            data: PaddedVec::from_vec(vec![10, 20, 30, 40]),
            null: Bitmap::all_set(4),
            missing: Bitmap::all_set(4),
        };
        let mut sel = Bitmap::all_unset(4);
        sel.set(0);
        sel.set(2);
        let batch = ColumnBatch {
            columns: vec![col],
            names: vec!["x".to_string()],
            selection: SelectionVector::Bitmap(sel),
            len: 4,
        };
        let schema = BatchSchema {
            names: vec!["x".to_string()],
            types: vec![ColumnType::Int32],
        };

        let count_agg = NamedAggregate::new(
            Aggregate::Count(crate::execution::types::CountAggregate::new(), Named::Star),
            Some("cnt".to_string()),
        );

        let registry = Arc::new(FunctionRegistry::new());
        let mut op = BatchGroupByOperator::new(
            Box::new(OneBatch { batch: Some(batch), schema }),
            vec![],
            vec![count_agg],
            LinkedHashMap::new(),
            registry,
        );

        let result = op.next_batch().unwrap().unwrap();
        assert_eq!(result.len, 1);
        let count_val = BatchToRowAdapter::extract_value(&result.columns[0], 0);
        assert_eq!(count_val, Value::Int(2));
    }
}
