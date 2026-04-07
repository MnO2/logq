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
}
