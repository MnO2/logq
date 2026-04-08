// src/execution/batch_groupby.rs

use crate::common;
use crate::common::types::{Tuple, Value, Variables};
use crate::execution::batch::{
    BatchSchema, BatchStream, BatchToRowAdapter, ColumnBatch, TypedColumn,
};
use crate::execution::types::{
    Aggregate, Named, NamedAggregate, StreamResult,
};
use crate::functions::FunctionRegistry;
use crate::simd::bitmap::Bitmap;
use crate::simd::selection::SelectionVector;
use crate::syntax::ast::PathExpr;
use linked_hash_map::LinkedHashMap;
use std::collections::hash_set;
use std::sync::Arc;

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

    /// Consume all child batches, feed rows into aggregates, then build the result batch.
    fn consume_and_build(&mut self) -> StreamResult<Option<ColumnBatch>> {
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
            group_counts.insert(status_str, count_int);
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
}
