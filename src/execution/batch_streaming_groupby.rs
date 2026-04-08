// src/execution/batch_streaming_groupby.rs

use std::sync::Arc;

use crate::common::types::{Value, Variables};
use crate::execution::batch::{
    BatchSchema, BatchStream, BatchToRowAdapter, ColumnBatch, ColumnType, TypedColumn,
};
use crate::execution::types::{
    AggregateDef, GroupState, NamedAggregate, StreamError, StreamResult,
};
use crate::functions::FunctionRegistry;
use crate::simd::bitmap::Bitmap;
use crate::simd::selection::SelectionVector;

const OUTPUT_BATCH_SIZE: usize = 64;

pub(crate) struct BatchStreamingGroupByOperator {
    input: Box<dyn BatchStream>,
    timestamp_column: String,
    bucket_interval: String,
    bucket_alias: String,
    aggregate_defs: Vec<AggregateDef>,
    aggregate_names: Vec<String>,
    current_key: Option<Value>,
    current_state: Option<GroupState>,
    completed_keys: Vec<Value>,
    completed_values: Vec<Vec<Value>>,
    input_exhausted: bool,
    variables: Variables,
    registry: Arc<FunctionRegistry>,
    output_schema: BatchSchema,
}

impl BatchStreamingGroupByOperator {
    pub(crate) fn new(
        input: Box<dyn BatchStream>,
        timestamp_column: String,
        bucket_interval: String,
        bucket_alias: String,
        aggregates: Vec<NamedAggregate>,
        variables: Variables,
        registry: Arc<FunctionRegistry>,
    ) -> Self {
        let aggregate_defs: Vec<AggregateDef> = aggregates
            .iter()
            .map(|na| AggregateDef::from_named_aggregate(na))
            .collect();

        let mut output_names = vec![bucket_alias.clone()];
        let mut aggregate_names = Vec::with_capacity(aggregates.len());
        for (idx, na) in aggregates.iter().enumerate() {
            let name = match &na.name_opt {
                Some(n) => n.clone(),
                None => format!("_{}", idx + 2),
            };
            aggregate_names.push(name.clone());
            output_names.push(name);
        }

        let types = vec![ColumnType::Mixed; output_names.len()];
        let output_schema = BatchSchema {
            names: output_names,
            types,
        };

        Self {
            input,
            timestamp_column,
            bucket_interval,
            bucket_alias,
            aggregate_defs,
            aggregate_names,
            current_key: None,
            current_state: None,
            completed_keys: Vec::new(),
            completed_values: Vec::new(),
            input_exhausted: false,
            variables,
            registry,
            output_schema,
        }
    }

    /// Compute the time bucket for a given timestamp value.
    pub(crate) fn compute_bucket(
        ts_val: &Value,
        interval: &str,
        registry: &Arc<FunctionRegistry>,
    ) -> Result<Value, StreamError> {
        let args = vec![Value::String(interval.to_string()), ts_val.clone()];
        registry
            .call("time_bucket", &args)
            .map_err(StreamError::Expression)
    }

    /// Finalize the current group and push key + aggregated values to
    /// the completed buffers.
    fn flush_current_group(&mut self) -> Result<(), StreamError> {
        if let (Some(key), Some(mut state)) = (self.current_key.take(), self.current_state.take()) {
            let mut values = Vec::with_capacity(state.accumulators.len());
            for acc in state.accumulators.iter_mut() {
                let val = acc.finalize().map_err(|_| StreamError::Aggregate)?;
                values.push(val);
            }
            self.completed_keys.push(key);
            self.completed_values.push(values);
        }
        Ok(())
    }

    /// Process one input batch: compute buckets, detect boundaries, accumulate.
    fn process_batch(&mut self, batch: &ColumnBatch) -> StreamResult<()> {
        // Find the timestamp column index
        let ts_idx = batch
            .names
            .iter()
            .position(|n| n == &self.timestamp_column)
            .ok_or_else(|| {
                StreamError::Expression(crate::execution::types::ExpressionError::KeyNotFound)
            })?;

        for row_idx in 0..batch.len {
            if !batch.selection.is_active(row_idx, batch.len) {
                continue;
            }

            // Extract timestamp value and compute bucket
            let ts_val = BatchToRowAdapter::extract_value(&batch.columns[ts_idx], row_idx);
            let bucket =
                Self::compute_bucket(&ts_val, &self.bucket_interval, &self.registry)?;

            // Check if this is the same bucket or a new one
            match &self.current_key {
                None => {
                    // First row ever
                    self.current_key = Some(bucket);
                    self.current_state = Some(GroupState::new(&self.aggregate_defs));
                }
                Some(current) if *current == bucket => {
                    // Same bucket -- continue accumulating
                }
                Some(_current) => {
                    // Out-of-order check: new bucket < current bucket
                    // Extract DateTime values for comparison since Value doesn't impl Ord
                    if let (Value::DateTime(new_dt), Some(Value::DateTime(cur_dt))) =
                        (&bucket, &self.current_key)
                    {
                        if new_dt < cur_dt {
                            return Err(StreamError::Expression(
                                crate::execution::types::ExpressionError::TypeMismatch,
                            ));
                        }
                    }
                    // New bucket -- finalize current and start new
                    self.flush_current_group()?;
                    self.current_key = Some(bucket);
                    self.current_state = Some(GroupState::new(&self.aggregate_defs));
                }
            }

            // Accumulate this row into the current group
            let state = self.current_state.as_mut().unwrap();
            for (agg_idx, agg_def) in self.aggregate_defs.iter().enumerate() {
                match &agg_def.extraction {
                    crate::execution::types::ExtractionStrategy::None => {
                        // COUNT(*)
                        state.accumulators[agg_idx].accumulate_row()?;
                    }
                    crate::execution::types::ExtractionStrategy::Expression(expr) => {
                        let mut row_vars =
                            linked_hash_map::LinkedHashMap::with_capacity(batch.columns.len());
                        for (col_idx, col) in batch.columns.iter().enumerate() {
                            let value = BatchToRowAdapter::extract_value(col, row_idx);
                            row_vars.insert(batch.names[col_idx].clone(), value);
                        }
                        let scope: Option<&crate::common::types::Variables> = if self.variables.is_empty() { None } else { Some(&self.variables) };
                        let val = expr.expression_value_impl(&row_vars, scope, &self.registry)?;
                        state.accumulators[agg_idx].accumulate(&val)?;
                    }
                    crate::execution::types::ExtractionStrategy::ColumnLookup(col_name) => {
                        let mut row_vars =
                            linked_hash_map::LinkedHashMap::with_capacity(batch.columns.len());
                        for (col_idx, col) in batch.columns.iter().enumerate() {
                            let value = BatchToRowAdapter::extract_value(col, row_idx);
                            row_vars.insert(batch.names[col_idx].clone(), value);
                        }
                        let scope: Option<&crate::common::types::Variables> = if self.variables.is_empty() { None } else { Some(&self.variables) };
                        let val = crate::common::types::scoped_get(&row_vars, scope, col_name)
                            .cloned()
                            .unwrap_or(Value::Missing);
                        state.accumulators[agg_idx].accumulate(&val)?;
                    }
                    crate::execution::types::ExtractionStrategy::RecordCapture => {
                        let mut row_vars =
                            linked_hash_map::LinkedHashMap::with_capacity(batch.columns.len());
                        for (col_idx, col) in batch.columns.iter().enumerate() {
                            let value = BatchToRowAdapter::extract_value(col, row_idx);
                            row_vars.insert(batch.names[col_idx].clone(), value);
                        }
                        let val = Value::Object(Box::new(row_vars));
                        state.accumulators[agg_idx].accumulate(&val)?;
                    }
                }
            }
        }
        Ok(())
    }

    /// Build a ColumnBatch from completed_keys and completed_values,
    /// draining them in the process.
    fn emit_output_batch(&mut self) -> Result<ColumnBatch, StreamError> {
        let n = self.completed_keys.len();

        // Key column
        let key_col = TypedColumn::Mixed {
            data: std::mem::take(&mut self.completed_keys),
            null: Bitmap::all_set(n),
            missing: Bitmap::all_set(n),
        };

        // Transpose row-major completed_values to column-major
        let num_aggs = self.aggregate_defs.len();
        let mut agg_columns: Vec<Vec<Value>> = vec![Vec::with_capacity(n); num_aggs];
        for row_vals in self.completed_values.drain(..) {
            for (col_idx, val) in row_vals.into_iter().enumerate() {
                agg_columns[col_idx].push(val);
            }
        }

        let mut columns = Vec::with_capacity(1 + num_aggs);
        columns.push(key_col);
        for col_data in agg_columns {
            let col_len = col_data.len();
            columns.push(TypedColumn::Mixed {
                data: col_data,
                null: Bitmap::all_set(col_len),
                missing: Bitmap::all_set(col_len),
            });
        }

        // Build names: bucket_alias + aggregate names
        let mut names = Vec::with_capacity(1 + num_aggs);
        names.push(self.bucket_alias.clone());
        names.extend(self.aggregate_names.iter().cloned());

        Ok(ColumnBatch {
            len: n,
            columns,
            names,
            selection: SelectionVector::All,
        })
    }
}

impl BatchStream for BatchStreamingGroupByOperator {
    fn next_batch(&mut self) -> StreamResult<Option<ColumnBatch>> {
        // If we have enough completed groups, emit them
        if self.completed_keys.len() >= OUTPUT_BATCH_SIZE {
            return Ok(Some(self.emit_output_batch()?));
        }

        // Process input batches until we have enough output or input is exhausted
        if !self.input_exhausted {
            loop {
                match self.input.next_batch()? {
                    Some(batch) => {
                        self.process_batch(&batch)?;
                        if self.completed_keys.len() >= OUTPUT_BATCH_SIZE {
                            return Ok(Some(self.emit_output_batch()?));
                        }
                    }
                    None => {
                        self.input_exhausted = true;
                        // Finalize the last group
                        self.flush_current_group()?;
                        break;
                    }
                }
            }
        }

        // Emit any remaining completed groups
        if !self.completed_keys.is_empty() {
            return Ok(Some(self.emit_output_batch()?));
        }

        Ok(None)
    }

    fn schema(&self) -> &BatchSchema {
        &self.output_schema
    }

    fn close(&self) {
        self.input.close();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::execution::batch::{BatchSchema, ColumnType};
    use crate::execution::types::{Aggregate, CountAggregate, Named, NamedAggregate, StreamResult};
    use crate::functions::FunctionRegistry;
    use linked_hash_map::LinkedHashMap;
    use std::collections::VecDeque;
    use std::sync::Arc;

    fn build_mixed_column(values: Vec<Value>) -> TypedColumn {
        let n = values.len();
        TypedColumn::Mixed {
            data: values,
            null: Bitmap::all_set(n),
            missing: Bitmap::all_set(n),
        }
    }

    struct MultiBatchStream {
        batches: VecDeque<ColumnBatch>,
        schema: BatchSchema,
    }

    impl BatchStream for MultiBatchStream {
        fn next_batch(&mut self) -> StreamResult<Option<ColumnBatch>> {
            Ok(self.batches.pop_front())
        }
        fn schema(&self) -> &BatchSchema {
            &self.schema
        }
        fn close(&self) {}
    }

    #[test]
    fn test_streaming_groupby_constructs() {
        let schema = BatchSchema {
            names: vec!["timestamp".into()],
            types: vec![ColumnType::Mixed],
        };
        let child = MultiBatchStream {
            batches: VecDeque::new(),
            schema,
        };
        let registry = Arc::new(FunctionRegistry::new());
        let variables = LinkedHashMap::new();

        let op = BatchStreamingGroupByOperator::new(
            Box::new(child),
            "timestamp".to_string(),
            "5 minutes".to_string(),
            "bucket".to_string(),
            vec![NamedAggregate::new(
                Aggregate::Count(CountAggregate::new(), Named::Star),
                Some("cnt".to_string()),
            )],
            variables,
            registry,
        );
        assert!(op.current_key.is_none());
        assert!(op.current_state.is_none());
    }

    #[test]
    fn test_compute_bucket() {
        let registry = Arc::new(crate::functions::register_all().unwrap());

        let ts = chrono::DateTime::parse_from_rfc3339("2026-04-07T10:13:45Z").unwrap();
        let ts_val = Value::DateTime(ts);

        let bucket =
            BatchStreamingGroupByOperator::compute_bucket(&ts_val, "5 minutes", &registry)
                .unwrap();

        let expected = chrono::DateTime::parse_from_rfc3339("2026-04-07T10:10:00Z").unwrap();
        assert_eq!(bucket, Value::DateTime(expected));
    }

    #[test]
    fn test_flush_completed_builds_batch() {
        let schema = BatchSchema {
            names: vec!["timestamp".into()],
            types: vec![ColumnType::Mixed],
        };
        let child = MultiBatchStream {
            batches: VecDeque::new(),
            schema,
        };
        let registry = Arc::new(FunctionRegistry::new());
        let variables = LinkedHashMap::new();

        let mut op = BatchStreamingGroupByOperator::new(
            Box::new(child),
            "timestamp".to_string(),
            "5 minutes".to_string(),
            "bucket".to_string(),
            vec![NamedAggregate::new(
                Aggregate::Count(CountAggregate::new(), Named::Star),
                Some("cnt".to_string()),
            )],
            variables,
            registry,
        );

        op.completed_keys
            .push(Value::String("bucket_a".to_string()));
        op.completed_values.push(vec![Value::Int(10)]);
        op.completed_keys
            .push(Value::String("bucket_b".to_string()));
        op.completed_values.push(vec![Value::Int(20)]);

        let batch = op.emit_output_batch().unwrap();
        assert_eq!(batch.len, 2);
        assert_eq!(batch.names[0], "bucket");
        assert_eq!(batch.names[1], "cnt");

        let k0 = BatchToRowAdapter::extract_value(&batch.columns[0], 0);
        assert_eq!(k0, Value::String("bucket_a".to_string()));
        let v0 = BatchToRowAdapter::extract_value(&batch.columns[1], 0);
        assert_eq!(v0, Value::Int(10));
    }

    #[test]
    fn test_streaming_groupby_single_batch_two_buckets() {
        let registry = Arc::new(crate::functions::register_all().unwrap());
        let variables = LinkedHashMap::new();

        let ts_values = vec![
            Value::DateTime(
                chrono::DateTime::parse_from_rfc3339("2026-04-07T10:01:00Z").unwrap(),
            ),
            Value::DateTime(
                chrono::DateTime::parse_from_rfc3339("2026-04-07T10:03:00Z").unwrap(),
            ),
            Value::DateTime(
                chrono::DateTime::parse_from_rfc3339("2026-04-07T10:06:00Z").unwrap(),
            ),
            Value::DateTime(
                chrono::DateTime::parse_from_rfc3339("2026-04-07T10:08:00Z").unwrap(),
            ),
        ];

        let ts_col = build_mixed_column(ts_values);
        let batch = ColumnBatch {
            columns: vec![ts_col],
            names: vec!["timestamp".to_string()],
            selection: SelectionVector::All,
            len: 4,
        };
        let schema = BatchSchema {
            names: vec!["timestamp".into()],
            types: vec![ColumnType::Mixed],
        };
        let child = MultiBatchStream {
            batches: VecDeque::from(vec![batch]),
            schema,
        };

        let mut op = BatchStreamingGroupByOperator::new(
            Box::new(child),
            "timestamp".to_string(),
            "5 minutes".to_string(),
            "bucket".to_string(),
            vec![NamedAggregate::new(
                Aggregate::Count(CountAggregate::new(), Named::Star),
                Some("cnt".to_string()),
            )],
            variables,
            registry,
        );

        let mut all_keys = Vec::new();
        let mut all_counts = Vec::new();
        loop {
            match op.next_batch().unwrap() {
                Some(b) => {
                    for i in 0..b.len {
                        all_keys.push(BatchToRowAdapter::extract_value(&b.columns[0], i));
                        all_counts.push(BatchToRowAdapter::extract_value(&b.columns[1], i));
                    }
                }
                None => break,
            }
        }

        assert_eq!(all_keys.len(), 2);
        let expected_bucket_1 = Value::DateTime(
            chrono::DateTime::parse_from_rfc3339("2026-04-07T10:00:00Z").unwrap(),
        );
        let expected_bucket_2 = Value::DateTime(
            chrono::DateTime::parse_from_rfc3339("2026-04-07T10:05:00Z").unwrap(),
        );
        assert_eq!(all_keys[0], expected_bucket_1);
        assert_eq!(all_counts[0], Value::Int(2));
        assert_eq!(all_keys[1], expected_bucket_2);
        assert_eq!(all_counts[1], Value::Int(2));
    }

    #[test]
    fn test_streaming_groupby_cross_batch_bucket() {
        let registry = Arc::new(crate::functions::register_all().unwrap());
        let variables = LinkedHashMap::new();

        // Batch 1: two rows in 10:00 bucket
        let batch1 = ColumnBatch {
            columns: vec![build_mixed_column(vec![
                Value::DateTime(
                    chrono::DateTime::parse_from_rfc3339("2026-04-07T10:01:00Z").unwrap(),
                ),
                Value::DateTime(
                    chrono::DateTime::parse_from_rfc3339("2026-04-07T10:03:00Z").unwrap(),
                ),
            ])],
            names: vec!["timestamp".to_string()],
            selection: SelectionVector::All,
            len: 2,
        };
        // Batch 2: one more row in 10:00 bucket, then one in 10:05
        let batch2 = ColumnBatch {
            columns: vec![build_mixed_column(vec![
                Value::DateTime(
                    chrono::DateTime::parse_from_rfc3339("2026-04-07T10:04:00Z").unwrap(),
                ),
                Value::DateTime(
                    chrono::DateTime::parse_from_rfc3339("2026-04-07T10:06:00Z").unwrap(),
                ),
            ])],
            names: vec!["timestamp".to_string()],
            selection: SelectionVector::All,
            len: 2,
        };

        let schema = BatchSchema {
            names: vec!["timestamp".into()],
            types: vec![ColumnType::Mixed],
        };
        let child = MultiBatchStream {
            batches: VecDeque::from(vec![batch1, batch2]),
            schema,
        };

        let mut op = BatchStreamingGroupByOperator::new(
            Box::new(child),
            "timestamp".to_string(),
            "5 minutes".to_string(),
            "bucket".to_string(),
            vec![NamedAggregate::new(
                Aggregate::Count(CountAggregate::new(), Named::Star),
                Some("cnt".to_string()),
            )],
            variables,
            registry,
        );

        let mut all_counts = Vec::new();
        loop {
            match op.next_batch().unwrap() {
                Some(b) => {
                    for i in 0..b.len {
                        all_counts.push(BatchToRowAdapter::extract_value(&b.columns[1], i));
                    }
                }
                None => break,
            }
        }

        assert_eq!(all_counts.len(), 2);
        assert_eq!(all_counts[0], Value::Int(3)); // 10:00 bucket: 3 rows spanning both batches
        assert_eq!(all_counts[1], Value::Int(1)); // 10:05 bucket: 1 row
    }

    #[test]
    fn test_streaming_groupby_empty_input() {
        let registry = Arc::new(crate::functions::register_all().unwrap());
        let variables = LinkedHashMap::new();
        let schema = BatchSchema {
            names: vec!["timestamp".into()],
            types: vec![ColumnType::Mixed],
        };
        let child = MultiBatchStream {
            batches: VecDeque::new(),
            schema,
        };

        let mut op = BatchStreamingGroupByOperator::new(
            Box::new(child),
            "timestamp".to_string(),
            "5 minutes".to_string(),
            "bucket".to_string(),
            vec![NamedAggregate::new(
                Aggregate::Count(CountAggregate::new(), Named::Star),
                Some("cnt".to_string()),
            )],
            variables,
            registry,
        );

        assert!(op.next_batch().unwrap().is_none());
    }

    #[test]
    fn test_streaming_groupby_single_bucket() {
        let registry = Arc::new(crate::functions::register_all().unwrap());
        let variables = LinkedHashMap::new();

        let batch = ColumnBatch {
            columns: vec![build_mixed_column(vec![
                Value::DateTime(chrono::DateTime::parse_from_rfc3339("2026-04-07T10:01:00Z").unwrap()),
                Value::DateTime(chrono::DateTime::parse_from_rfc3339("2026-04-07T10:02:00Z").unwrap()),
                Value::DateTime(chrono::DateTime::parse_from_rfc3339("2026-04-07T10:04:00Z").unwrap()),
            ])],
            names: vec!["timestamp".to_string()],
            selection: SelectionVector::All,
            len: 3,
        };

        let schema = BatchSchema { names: vec!["timestamp".into()], types: vec![ColumnType::Mixed] };
        let child = MultiBatchStream { batches: VecDeque::from(vec![batch]), schema };

        let mut op = BatchStreamingGroupByOperator::new(
            Box::new(child),
            "timestamp".to_string(),
            "5 minutes".to_string(),
            "bucket".to_string(),
            vec![NamedAggregate::new(
                Aggregate::Count(CountAggregate::new(), Named::Star),
                Some("cnt".to_string()),
            )],
            variables,
            registry,
        );

        let result = op.next_batch().unwrap().unwrap();
        assert_eq!(result.len, 1);
        assert_eq!(BatchToRowAdapter::extract_value(&result.columns[1], 0), Value::Int(3));
        assert!(op.next_batch().unwrap().is_none());
    }

    #[test]
    fn test_streaming_groupby_with_sum_aggregate() {
        use crate::execution::types::{SumAggregate, Expression};
        use crate::syntax::ast::{PathExpr, PathSegment};
        use ordered_float::OrderedFloat;

        let registry = Arc::new(crate::functions::register_all().unwrap());
        let variables = LinkedHashMap::new();

        let ts_col = build_mixed_column(vec![
            Value::DateTime(chrono::DateTime::parse_from_rfc3339("2026-04-07T10:01:00Z").unwrap()),
            Value::DateTime(chrono::DateTime::parse_from_rfc3339("2026-04-07T10:03:00Z").unwrap()),
            Value::DateTime(chrono::DateTime::parse_from_rfc3339("2026-04-07T10:06:00Z").unwrap()),
            Value::DateTime(chrono::DateTime::parse_from_rfc3339("2026-04-07T10:08:00Z").unwrap()),
        ]);
        let bytes_col = build_mixed_column(vec![
            Value::Int(100), Value::Int(200), Value::Int(300), Value::Int(400),
        ]);
        let batch = ColumnBatch {
            columns: vec![ts_col, bytes_col],
            names: vec!["timestamp".to_string(), "bytes".to_string()],
            selection: SelectionVector::All,
            len: 4,
        };

        let schema = BatchSchema {
            names: vec!["timestamp".into(), "bytes".into()],
            types: vec![ColumnType::Mixed, ColumnType::Mixed],
        };
        let child = MultiBatchStream { batches: VecDeque::from(vec![batch]), schema };

        let bytes_path = PathExpr::new(vec![PathSegment::AttrName("bytes".to_string())]);
        let sum_agg = NamedAggregate::new(
            Aggregate::Sum(
                SumAggregate::new(),
                Named::Expression(
                    Expression::Variable(bytes_path),
                    None,
                ),
            ),
            Some("total".to_string()),
        );

        let mut op = BatchStreamingGroupByOperator::new(
            Box::new(child),
            "timestamp".to_string(),
            "5 minutes".to_string(),
            "bucket".to_string(),
            vec![
                NamedAggregate::new(
                    Aggregate::Count(CountAggregate::new(), Named::Star),
                    Some("cnt".to_string()),
                ),
                sum_agg,
            ],
            variables,
            registry,
        );

        let mut rows: Vec<(Value, Value, Value)> = Vec::new();
        loop {
            match op.next_batch().unwrap() {
                Some(b) => {
                    for i in 0..b.len {
                        rows.push((
                            BatchToRowAdapter::extract_value(&b.columns[0], i),
                            BatchToRowAdapter::extract_value(&b.columns[1], i),
                            BatchToRowAdapter::extract_value(&b.columns[2], i),
                        ));
                    }
                }
                None => break,
            }
        }

        assert_eq!(rows.len(), 2);
        // Bucket 10:00: count=2, sum=300
        assert_eq!(rows[0].1, Value::Int(2));
        assert_eq!(rows[0].2, Value::Float(OrderedFloat(300.0f32)));
        // Bucket 10:05: count=2, sum=700
        assert_eq!(rows[1].1, Value::Int(2));
        assert_eq!(rows[1].2, Value::Float(OrderedFloat(700.0f32)));
    }

    #[test]
    fn test_streaming_groupby_many_buckets() {
        let registry = Arc::new(crate::functions::register_all().unwrap());
        let variables = LinkedHashMap::new();

        // Create 100 timestamps, each in a different 1-second bucket
        let base = chrono::DateTime::parse_from_rfc3339("2026-04-07T10:00:00Z").unwrap();
        let ts_values: Vec<Value> = (0..100)
            .map(|i| {
                Value::DateTime(base + chrono::Duration::seconds(i))
            })
            .collect();

        let batch = ColumnBatch {
            columns: vec![build_mixed_column(ts_values)],
            names: vec!["timestamp".to_string()],
            selection: SelectionVector::All,
            len: 100,
        };

        let schema = BatchSchema { names: vec!["timestamp".into()], types: vec![ColumnType::Mixed] };
        let child = MultiBatchStream { batches: VecDeque::from(vec![batch]), schema };

        let mut op = BatchStreamingGroupByOperator::new(
            Box::new(child),
            "timestamp".to_string(),
            "1 second".to_string(),
            "bucket".to_string(),
            vec![NamedAggregate::new(
                Aggregate::Count(CountAggregate::new(), Named::Star),
                Some("cnt".to_string()),
            )],
            variables,
            registry,
        );

        let mut total_groups = 0;
        let mut batch_count = 0;
        loop {
            match op.next_batch().unwrap() {
                Some(b) => {
                    total_groups += b.len;
                    batch_count += 1;
                    // Each group should have count=1 (one timestamp per second)
                    for i in 0..b.len {
                        assert_eq!(BatchToRowAdapter::extract_value(&b.columns[1], i), Value::Int(1));
                    }
                }
                None => break,
            }
        }

        assert_eq!(total_groups, 100);
        // With OUTPUT_BATCH_SIZE=64, should be 2 output batches (64 + 36)
        assert_eq!(batch_count, 2);
    }
}
