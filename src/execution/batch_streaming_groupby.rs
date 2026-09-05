// Raw log formats do not establish an ordering contract for a query. Compute
// time buckets once per row, then use the shared hash aggregator so repeated,
// unsorted, and NULL/MISSING-separated buckets remain one logical group.
use std::sync::Arc;

use crate::common::types::{Value, Variables};
use crate::execution::batch::{BatchSchema, BatchStream, BatchToRowAdapter, ColumnBatch, ColumnType, TypedColumn};
use crate::execution::batch_groupby::BatchGroupByOperator;
use crate::execution::memory::MemoryTracker;
use crate::execution::types::{ExpressionError, NamedAggregate, StreamError, StreamResult};
use crate::functions::FunctionRegistry;
use crate::functions::datetime::CompiledTimeBucket;
use crate::simd::bitmap::Bitmap;
use crate::syntax::ast::{PathExpr, PathSegment};

pub(crate) struct BatchStreamingGroupByOperator {
    grouped: BatchGroupByOperator,
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
        let mut schema = input.schema().clone();
        schema.names.push(bucket_alias.clone());
        schema.types.push(ColumnType::Mixed);
        let projection = TimeBucketProjection {
            input,
            timestamp_column,
            interval: bucket_interval,
            alias: bucket_alias.clone(),
            compiled: None,
            schema,
            variables: variables.clone(),
        };
        Self {
            grouped: BatchGroupByOperator::new(
                Box::new(projection),
                vec![PathExpr::new(vec![PathSegment::AttrName(bucket_alias)])],
                aggregates,
                variables,
                registry,
            ),
        }
    }

    pub(crate) fn with_memory_tracker(mut self, memory: MemoryTracker) -> Self {
        self.grouped = self.grouped.with_memory_tracker(memory);
        self
    }

    #[cfg(test)]
    pub(crate) fn compute_bucket(
        timestamp: &Value,
        interval: &str,
        registry: &Arc<FunctionRegistry>,
    ) -> StreamResult<Value> {
        registry
            .call("time_bucket", &[Value::String(interval.into()), timestamp.clone()])
            .map_err(StreamError::Expression)
    }
}

impl BatchStream for BatchStreamingGroupByOperator {
    fn next_batch(&mut self) -> StreamResult<Option<ColumnBatch>> {
        self.grouped.next_batch()
    }
    fn schema(&self) -> &BatchSchema {
        self.grouped.schema()
    }
    fn close(&self) {
        self.grouped.close();
    }
}

/// Preserve the source columns for aggregate extraction, appending the computed
/// bucket as the last column so an alias shadows an existing source name just
/// as the row projection's map insertion does.
struct TimeBucketProjection {
    input: Box<dyn BatchStream>,
    timestamp_column: String,
    interval: String,
    alias: String,
    compiled: Option<CompiledTimeBucket>,
    schema: BatchSchema,
    variables: Variables,
}

impl TimeBucketProjection {
    fn bucket(&mut self, timestamp: &Value) -> StreamResult<Value> {
        match timestamp {
            Value::Null => Ok(Value::Null),
            Value::Missing => Ok(Value::Missing),
            Value::DateTime(timestamp) => {
                // Scalar null propagation never evaluates an invalid interval
                // for NULL/MISSING timestamps, or on an empty input stream.
                if self.compiled.is_none() {
                    self.compiled = Some(CompiledTimeBucket::parse(&self.interval)?);
                }
                Ok(Value::DateTime(self.compiled.as_ref().unwrap().apply(timestamp)?))
            }
            _ => Err(StreamError::Expression(ExpressionError::InvalidArguments)),
        }
    }
}

impl BatchStream for TimeBucketProjection {
    fn next_batch(&mut self) -> StreamResult<Option<ColumnBatch>> {
        let Some(mut batch) = self.input.next_batch()? else {
            return Ok(None);
        };
        let timestamp_index = batch.names.iter().rposition(|name| name == &self.timestamp_column);
        let mut data = Vec::with_capacity(batch.len);
        let mut null = Bitmap::all_set(batch.len);
        let mut missing = Bitmap::all_set(batch.len);
        for row in 0..batch.len {
            if !batch.selection.is_active(row, batch.len) {
                data.push(Value::Missing);
                missing.unset(row);
                continue;
            }
            let value = match timestamp_index {
                Some(index) => BatchToRowAdapter::extract_value(&batch.columns[index], row),
                None => self
                    .variables
                    .get(&self.timestamp_column)
                    .cloned()
                    .unwrap_or(Value::Missing),
            };
            let bucket = self.bucket(&value)?;
            match bucket {
                Value::Null => null.unset(row),
                Value::Missing => missing.unset(row),
                _ => {}
            }
            data.push(bucket);
        }
        batch.columns.push(TypedColumn::Mixed { data, null, missing });
        batch.names.push(self.alias.clone());
        Ok(Some(batch))
    }
    fn schema(&self) -> &BatchSchema {
        &self.schema
    }
    fn close(&self) {
        self.input.close();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::execution::batch::{BatchSchema, ColumnType};
    use crate::execution::memory::estimate_batch;
    use crate::execution::types::{Aggregate, CountAggregate, Expression, Named, NamedAggregate, StreamResult};
    use crate::functions::FunctionRegistry;
    use crate::simd::selection::SelectionVector;
    use linked_hash_map::LinkedHashMap;
    use std::collections::VecDeque;
    use std::sync::Arc;

    fn make_operator(timestamps: Vec<Value>, interval: &str) -> BatchStreamingGroupByOperator {
        let len = timestamps.len();
        let schema = BatchSchema {
            names: vec!["timestamp".into()],
            types: vec![ColumnType::Mixed],
        };
        let batches = if len == 0 {
            VecDeque::new()
        } else {
            VecDeque::from([ColumnBatch {
                columns: vec![build_mixed_column(timestamps)],
                names: schema.names.clone(),
                selection: SelectionVector::All,
                len,
            }])
        };
        BatchStreamingGroupByOperator::new(
            Box::new(MultiBatchStream { batches, schema }),
            "timestamp".into(),
            interval.into(),
            "bucket".into(),
            vec![NamedAggregate::new(
                Aggregate::Count(CountAggregate::new(), Named::Star),
                Some("n".into()),
            )],
            Variables::new(),
            Arc::new(crate::functions::register_all().unwrap()),
        )
    }

    #[test]
    fn streaming_budget_failure_releases_retained_state() {
        let tracker = MemoryTracker::new(Some(1));
        let mut op = make_operator(vec![Value::Null], "5m").with_memory_tracker(tracker.clone());
        assert!(matches!(op.next_batch(), Err(StreamError::MemoryBudgetExceeded)));
        assert_eq!(tracker.used(), 0);
        // A failed operator must stay exhausted instead of reserving again.
        assert!(op.next_batch().unwrap().is_none());
    }

    #[test]
    fn streaming_budget_tracks_output_until_next_pull_or_drop() {
        for finish in [true, false] {
            let tracker = MemoryTracker::new(Some(1024 * 1024));
            let mut op = make_operator(vec![Value::Null; 100], "5m").with_memory_tracker(tracker.clone());
            let output = op.next_batch().unwrap().unwrap();
            assert!(tracker.used() >= estimate_batch(&output));
            assert_eq!(BatchToRowAdapter::extract_value(&output.columns[1], 0), Value::Int(100));
            if finish {
                assert!(op.next_batch().unwrap().is_none());
                assert_eq!(tracker.used(), 0);
            }
            drop(op);
            assert_eq!(tracker.used(), 0);
        }
    }

    #[test]
    fn streaming_invalid_interval_is_lazy_for_empty_null_and_missing_input() {
        let mut empty = make_operator(vec![], "invalid");
        assert!(empty.next_batch().unwrap().is_none());
        for value in [Value::Null, Value::Missing] {
            let mut op = make_operator(vec![value.clone()], "invalid");
            let batch = op.next_batch().unwrap().unwrap();
            assert_eq!(BatchToRowAdapter::extract_value(&batch.columns[0], 0), value);
        }
        let mut wrong_type = make_operator(vec![Value::Int(1)], "invalid");
        assert!(matches!(
            wrong_type.next_batch(),
            Err(StreamError::Expression(
                crate::execution::types::ExpressionError::InvalidArguments
            ))
        ));
        let mut invalid_interval = make_operator(
            vec![Value::DateTime(
                chrono::DateTime::parse_from_rfc3339("2026-04-07T10:01:00Z").unwrap(),
            )],
            "0s",
        );
        assert!(matches!(
            invalid_interval.next_batch(),
            Err(StreamError::Expression(
                crate::execution::types::ExpressionError::TimeIntervalZero
            ))
        ));
    }

    #[test]
    fn time_bucket_accepts_out_of_order_input_after_truncation() {
        let later = Value::DateTime(chrono::DateTime::parse_from_rfc3339("2026-04-07T10:06:00+05:45").unwrap());
        let earlier = Value::DateTime(chrono::DateTime::parse_from_rfc3339("2026-04-07T10:01:00+05:45").unwrap());
        let mut op = make_operator(vec![later, earlier], "5m");
        let output = op.next_batch().unwrap().unwrap();
        assert_eq!(output.len, 2);
        for row in 0..2 {
            assert_eq!(BatchToRowAdapter::extract_value(&output.columns[1], row), Value::Int(1));
        }
    }

    #[test]
    fn fixed_format_timestamp_scan_preserves_offset_nanoseconds_before_bucketing() {
        let original = chrono::DateTime::parse_from_rfc3339("1969-12-31T23:59:59.123456789-03:30").unwrap();
        let mut scan = crate::execution::batch_scan::BatchScanOperator::new(
            Box::new(std::io::Cursor::new(
                format!("{} elb", original.to_rfc3339()).into_bytes(),
            )),
            crate::execution::log_schema::LogSchema::from_format("elb"),
            vec![0],
            vec![],
            None,
        );
        let batch = scan.next_batch().unwrap().unwrap();
        assert!(matches!(batch.columns[0], TypedColumn::Mixed { .. }));
        let Value::DateTime(parsed) = BatchToRowAdapter::extract_value(&batch.columns[0], 0) else {
            panic!("expected timestamp")
        };
        assert_eq!(parsed.to_rfc3339(), original.to_rfc3339());
        let mut op = make_operator(vec![Value::DateTime(parsed)], "5m");
        let result = op.next_batch().unwrap().unwrap();
        let Value::DateTime(bucket) = BatchToRowAdapter::extract_value(&result.columns[0], 0) else {
            panic!("expected bucket")
        };
        assert_eq!(bucket.to_rfc3339(), "1969-12-31T23:55:00-03:30");
    }

    #[test]
    fn streaming_aggregates_preserve_scope_nulls_and_complex_expressions() {
        use crate::execution::types::{AvgAggregate, SumAggregate};
        use crate::syntax::ast::{PathExpr, PathSegment};
        use ordered_float::OrderedFloat;

        let variable = |name: &str| {
            Named::Expression(
                Expression::Variable(PathExpr::new(vec![PathSegment::AttrName(name.into())])),
                None,
            )
        };
        let timestamp =
            Value::DateTime(chrono::DateTime::parse_from_rfc3339("2026-04-07T10:03:47.123456789+05:45").unwrap());
        let schema = BatchSchema {
            names: vec!["timestamp".into(), "bytes".into()],
            types: vec![ColumnType::Mixed; 2],
        };
        let mut selected = Bitmap::all_set(3);
        selected.unset(2);
        let batch = ColumnBatch {
            columns: vec![
                build_mixed_column(vec![
                    timestamp.clone(),
                    timestamp,
                    Value::String("not evaluated".into()),
                ]),
                build_mixed_column(vec![Value::Int(10), Value::Null, Value::Int(999)]),
            ],
            names: schema.names.clone(),
            selection: SelectionVector::Bitmap(selected),
            len: 3,
        };
        let mut variables = Variables::new();
        variables.insert("bytes".into(), Value::Int(500));
        variables.insert("fallback".into(), Value::Int(4));
        let aggregates = vec![
            NamedAggregate::new(
                Aggregate::Count(CountAggregate::new(), variable("bytes")),
                Some("count".into()),
            ),
            NamedAggregate::new(
                Aggregate::Sum(SumAggregate::new(), variable("bytes")),
                Some("sum".into()),
            ),
            NamedAggregate::new(
                Aggregate::Avg(AvgAggregate::new(), variable("bytes")),
                Some("avg".into()),
            ),
            NamedAggregate::new(
                Aggregate::Sum(SumAggregate::new(), variable("fallback")),
                Some("scope_sum".into()),
            ),
            NamedAggregate::new(
                Aggregate::Sum(
                    SumAggregate::new(),
                    Named::Expression(
                        Expression::Function("plus".into(), vec![variable("bytes"), variable("fallback")]),
                        None,
                    ),
                ),
                Some("expression_sum".into()),
            ),
        ];
        let mut op = BatchStreamingGroupByOperator::new(
            Box::new(MultiBatchStream {
                batches: VecDeque::from([batch]),
                schema,
            }),
            "timestamp".into(),
            "5m".into(),
            "bucket".into(),
            aggregates,
            variables,
            Arc::new(crate::functions::register_all().unwrap()),
        );
        let result = op.next_batch().unwrap().unwrap();
        let expected = [
            Value::Int(1),
            Value::Float(OrderedFloat(10.0)),
            Value::Float(OrderedFloat(10.0)),
            Value::Float(OrderedFloat(8.0)),
            Value::Float(OrderedFloat(14.0)),
        ];
        for (column, expected) in result.columns.iter().skip(1).zip(expected) {
            assert_eq!(BatchToRowAdapter::extract_value(column, 0), expected);
        }
        let Value::DateTime(bucket) = BatchToRowAdapter::extract_value(&result.columns[0], 0) else {
            panic!("expected timestamp")
        };
        assert_eq!(bucket.to_rfc3339(), "2026-04-07T10:00:00+05:45");
    }

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
        assert_eq!(op.schema().names, vec!["bucket", "cnt"]);
        assert_eq!(op.schema().types, vec![ColumnType::Mixed; 2]);
    }

    #[test]
    fn test_compute_bucket() {
        let registry = Arc::new(crate::functions::register_all().unwrap());

        let ts = chrono::DateTime::parse_from_rfc3339("2026-04-07T10:13:45Z").unwrap();
        let ts_val = Value::DateTime(ts);

        let bucket = BatchStreamingGroupByOperator::compute_bucket(&ts_val, "5 minutes", &registry).unwrap();

        let expected = chrono::DateTime::parse_from_rfc3339("2026-04-07T10:10:00Z").unwrap();
        assert_eq!(bucket, Value::DateTime(expected));
    }

    #[test]
    fn test_streaming_groupby_single_batch_two_buckets() {
        let registry = Arc::new(crate::functions::register_all().unwrap());
        let variables = LinkedHashMap::new();

        let ts_values = vec![
            Value::DateTime(chrono::DateTime::parse_from_rfc3339("2026-04-07T10:01:00Z").unwrap()),
            Value::DateTime(chrono::DateTime::parse_from_rfc3339("2026-04-07T10:03:00Z").unwrap()),
            Value::DateTime(chrono::DateTime::parse_from_rfc3339("2026-04-07T10:06:00Z").unwrap()),
            Value::DateTime(chrono::DateTime::parse_from_rfc3339("2026-04-07T10:08:00Z").unwrap()),
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
        while let Some(b) = op.next_batch().unwrap() {
            for i in 0..b.len {
                all_keys.push(BatchToRowAdapter::extract_value(&b.columns[0], i));
                all_counts.push(BatchToRowAdapter::extract_value(&b.columns[1], i));
            }
        }

        assert_eq!(all_keys.len(), 2);
        let expected_bucket_1 = Value::DateTime(chrono::DateTime::parse_from_rfc3339("2026-04-07T10:00:00Z").unwrap());
        let expected_bucket_2 = Value::DateTime(chrono::DateTime::parse_from_rfc3339("2026-04-07T10:05:00Z").unwrap());
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
                Value::DateTime(chrono::DateTime::parse_from_rfc3339("2026-04-07T10:01:00Z").unwrap()),
                Value::DateTime(chrono::DateTime::parse_from_rfc3339("2026-04-07T10:03:00Z").unwrap()),
            ])],
            names: vec!["timestamp".to_string()],
            selection: SelectionVector::All,
            len: 2,
        };
        // Batch 2: one more row in 10:00 bucket, then one in 10:05
        let batch2 = ColumnBatch {
            columns: vec![build_mixed_column(vec![
                Value::DateTime(chrono::DateTime::parse_from_rfc3339("2026-04-07T10:04:00Z").unwrap()),
                Value::DateTime(chrono::DateTime::parse_from_rfc3339("2026-04-07T10:06:00Z").unwrap()),
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
        while let Some(b) = op.next_batch().unwrap() {
            for i in 0..b.len {
                all_counts.push(BatchToRowAdapter::extract_value(&b.columns[1], i));
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

        let result = op.next_batch().unwrap().unwrap();
        assert_eq!(result.len, 1);
        assert_eq!(BatchToRowAdapter::extract_value(&result.columns[1], 0), Value::Int(3));
        assert!(op.next_batch().unwrap().is_none());
    }

    #[test]
    fn test_streaming_groupby_with_sum_aggregate() {
        use crate::execution::types::{Expression, SumAggregate};
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
        let bytes_col = build_mixed_column(vec![Value::Int(100), Value::Int(200), Value::Int(300), Value::Int(400)]);
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
        let child = MultiBatchStream {
            batches: VecDeque::from(vec![batch]),
            schema,
        };

        let bytes_path = PathExpr::new(vec![PathSegment::AttrName("bytes".to_string())]);
        let sum_agg = NamedAggregate::new(
            Aggregate::Sum(
                SumAggregate::new(),
                Named::Expression(Expression::Variable(bytes_path), None),
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
        while let Some(b) = op.next_batch().unwrap() {
            for i in 0..b.len {
                rows.push((
                    BatchToRowAdapter::extract_value(&b.columns[0], i),
                    BatchToRowAdapter::extract_value(&b.columns[1], i),
                    BatchToRowAdapter::extract_value(&b.columns[2], i),
                ));
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
            .map(|i| Value::DateTime(base + chrono::Duration::seconds(i)))
            .collect();

        let batch = ColumnBatch {
            columns: vec![build_mixed_column(ts_values)],
            names: vec!["timestamp".to_string()],
            selection: SelectionVector::All,
            len: 100,
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
        while let Some(b) = op.next_batch().unwrap() {
            total_groups += b.len;
            // Each group should have count=1 (one timestamp per second)
            for i in 0..b.len {
                assert_eq!(BatchToRowAdapter::extract_value(&b.columns[1], i), Value::Int(1));
            }
        }

        assert_eq!(total_groups, 100);
    }
    #[test]
    fn streaming_group_as_budget_fails_before_reading_another_batch() {
        struct SentinelAfterBatch {
            batch: Option<ColumnBatch>,
            schema: BatchSchema,
        }
        impl BatchStream for SentinelAfterBatch {
            fn next_batch(&mut self) -> StreamResult<Option<ColumnBatch>> {
                self.batch
                    .take()
                    .map(Some)
                    .ok_or_else(|| StreamError::General("read beyond retained budget".into()))
            }
            fn schema(&self) -> &BatchSchema {
                &self.schema
            }
            fn close(&self) {}
        }
        let timestamp = chrono::DateTime::parse_from_rfc3339("2024-01-01T00:00:00Z").unwrap();
        let input = ColumnBatch {
            columns: vec![
                build_mixed_column(vec![Value::DateTime(timestamp); 32]),
                build_mixed_column(vec![Value::String("x".repeat(500).into()); 32]),
            ],
            names: vec!["timestamp".into(), "payload".into()],
            selection: SelectionVector::All,
            len: 32,
        };
        let schema = BatchSchema {
            names: input.names.clone(),
            types: vec![ColumnType::Mixed; 2],
        };
        let mut op = BatchStreamingGroupByOperator::new(
            Box::new(SentinelAfterBatch {
                batch: Some(input),
                schema,
            }),
            "timestamp".into(),
            "1 minute".into(),
            "bucket".into(),
            vec![NamedAggregate::new(
                Aggregate::GroupAs(crate::execution::types::GroupAsAggregate::new(), Named::Star),
                Some("rows".into()),
            )],
            Variables::new(),
            Arc::new(crate::functions::register_all().unwrap()),
        );
        let memory = MemoryTracker::new(Some(4096));
        op = op.with_memory_tracker(memory.clone());
        assert!(matches!(op.next_batch(), Err(StreamError::MemoryBudgetExceeded)));
        assert_eq!(memory.used(), 0);
    }
    #[test]
    fn time_bucket_groups_repeated_null_and_unsorted_keys_once() {
        let t0 = Value::DateTime(chrono::DateTime::parse_from_rfc3339("2024-01-01T00:00:00Z").unwrap());
        let t1 = Value::DateTime(chrono::DateTime::parse_from_rfc3339("2024-01-01T00:01:00Z").unwrap());
        let mut op = make_operator(
            vec![
                t1.clone(),
                Value::Null,
                t1.clone(),
                Value::Missing,
                t0.clone(),
                t0.clone(),
                Value::Null,
            ],
            "1 minute",
        );
        let mut actual = std::collections::HashMap::new();
        while let Some(batch) = op.next_batch().unwrap() {
            for row in 0..batch.len {
                let key = BatchToRowAdapter::extract_value(&batch.columns[0], row);
                let count = BatchToRowAdapter::extract_value(&batch.columns[1], row);
                assert!(actual.insert(key, count).is_none(), "bucket emitted more than once");
            }
        }
        assert_eq!(
            actual,
            std::collections::HashMap::from([
                (t1, Value::Int(2)),
                (Value::Null, Value::Int(2)),
                (Value::Missing, Value::Int(1)),
                (t0, Value::Int(2))
            ])
        );
    }
}
