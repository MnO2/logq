// src/execution/batch_orderby.rs

use crate::common::types::Value;
use crate::execution::batch::*;
use crate::execution::memory::{MemoryReservation, MemoryTracker, estimate_batch, estimate_value};
use crate::execution::prefix_sort::{BoundedTopN, compare_values};
use crate::execution::stream::Record;
use crate::execution::types::{Ordering, StreamResult};
use crate::simd::bitmap::Bitmap;
use crate::simd::selection::SelectionVector;
use crate::syntax::ast::{PathExpr, PathSegment};
use std::cmp;

/// Batch-native ORDER BY operator. Consumes all batches, compacts active rows
/// into dense columns, sorts using a permutation index on columnar comparators,
/// then emits sorted batches.
pub(crate) struct BatchOrderByOperator {
    child: Box<dyn BatchStream>,
    sort_columns: Vec<PathExpr>,
    orderings: Vec<Ordering>,
    schema: BatchSchema,
    consumed: bool,
    result_batches: Vec<ColumnBatch>,
    emit_idx: usize,
    limit: Option<usize>,
    memory: MemoryReservation,
    #[cfg(test)]
    peak_retained_rows: usize,
}

impl BatchOrderByOperator {
    pub fn new(child: Box<dyn BatchStream>, sort_columns: Vec<PathExpr>, orderings: Vec<Ordering>) -> Self {
        Self::with_limit(child, sort_columns, orderings, None)
    }

    pub fn new_top_n(
        child: Box<dyn BatchStream>,
        sort_columns: Vec<PathExpr>,
        orderings: Vec<Ordering>,
        limit: usize,
    ) -> Self {
        Self::with_limit(child, sort_columns, orderings, Some(limit))
    }

    fn with_limit(
        child: Box<dyn BatchStream>,
        sort_columns: Vec<PathExpr>,
        orderings: Vec<Ordering>,
        limit: Option<usize>,
    ) -> Self {
        let schema = BatchSchema {
            names: child.schema().names.clone(),
            types: child.schema().types.clone(),
        };
        Self {
            child,
            sort_columns,
            orderings,
            schema,
            consumed: false,
            result_batches: Vec::new(),
            emit_idx: 0,
            limit,
            memory: MemoryReservation::default(),
            #[cfg(test)]
            peak_retained_rows: 0,
        }
    }

    pub(crate) fn with_memory_tracker(mut self, memory: MemoryTracker) -> Self {
        self.memory = MemoryReservation::new(memory);
        self
    }

    fn consume_and_sort(&mut self) -> StreamResult<()> {
        if let Some(limit) = self.limit {
            return self.consume_top_n(limit);
        }

        // Phase 1: Collect all active rows into accumulated columns
        let mut all_values: Vec<Vec<Value>> = Vec::new();
        let num_cols = self.schema.names.len();
        for _ in 0..num_cols {
            all_values.push(Vec::new());
        }

        let track_memory = self.memory.is_enabled();
        while let Some(mut batch) = self.child.next_batch()? {
            let mut input_memory = MemoryReservation::new(self.memory.tracker());
            if track_memory {
                input_memory.add(estimate_batch(&batch))?;
            }
            for row in 0..batch.len {
                if !batch.selection.is_active(row, batch.len) {
                    continue;
                }
                for (col_idx, col) in batch.columns.iter_mut().enumerate() {
                    let val = take_value(col, row);
                    if track_memory {
                        // Covers growing input columns plus the later gather's
                        // destination slots; payload ownership is moved once.
                        self.memory
                            .add(estimate_value(&val).saturating_add(3 * std::mem::size_of::<Value>()))?;
                    }
                    all_values[col_idx].push(val);
                }
            }
        }

        let total_rows = if num_cols > 0 { all_values[0].len() } else { 0 };
        if total_rows == 0 {
            return Ok(());
        }

        // Phase 2: Resolve sort column indices
        let sort_col_indices: Vec<Option<usize>> = self
            .sort_columns
            .iter()
            .map(|path| {
                if let Some(PathSegment::AttrName(name)) = path.path_segments.last() {
                    self.schema.names.iter().rposition(|n| n == name)
                } else {
                    None
                }
            })
            .collect();

        // Phase 3: Build permutation index and sort
        if track_memory {
            // Permutation plus the stable sort's scratch space.
            self.memory
                .add(total_rows.saturating_mul(2 * std::mem::size_of::<usize>()))?;
        }
        let mut indices: Vec<usize> = (0..total_rows).collect();
        indices.sort_by(|&a, &b| {
            for (i, col_idx_opt) in sort_col_indices.iter().enumerate() {
                let col_idx = match col_idx_opt {
                    Some(idx) => *idx,
                    None => continue,
                };
                let va = &all_values[col_idx][a];
                let vb = &all_values[col_idx][b];
                let ord = compare_values(va, vb);
                if ord == cmp::Ordering::Equal {
                    continue;
                }
                let ordering = self.orderings.get(i).copied().unwrap_or(Ordering::Asc);
                return match ordering {
                    Ordering::Asc => ord,
                    Ordering::Desc => ord.reverse(),
                };
            }
            cmp::Ordering::Equal
        });

        // Phase 4: Scatter into sorted order and emit as batches
        let mut sorted_values: Vec<Vec<Value>> = Vec::with_capacity(num_cols);
        for values in all_values.iter_mut().take(num_cols) {
            let mut col_data = Vec::with_capacity(total_rows);
            for &idx in &indices {
                col_data.push(std::mem::replace(&mut values[idx], Value::Missing));
            }
            sorted_values.push(col_data);
        }

        drop(all_values);
        drop(indices);
        self.emit_sorted_values(sorted_values, total_rows)?;

        Ok(())
    }

    fn consume_top_n(&mut self, limit: usize) -> StreamResult<()> {
        let mut top_n = BoundedTopN::new(limit, self.sort_columns.clone(), self.orderings.clone());
        let simple_keys = self
            .sort_columns
            .iter()
            .all(|path| matches!(path.path_segments.as_slice(), [PathSegment::AttrName(_)]));
        let key_indices: Vec<_> = self
            .sort_columns
            .iter()
            .map(|path| {
                path.path_segments.first().and_then(|part| match part {
                    PathSegment::AttrName(name) => self.schema.names.iter().rposition(|column| column == name),
                    _ => None,
                })
            })
            .collect();
        let mut keys = Vec::with_capacity(self.sort_columns.len());
        let track_memory = self.memory.is_enabled();
        while let Some(mut batch) = self.child.next_batch()? {
            let mut input_memory = MemoryReservation::new(self.memory.tracker());
            if track_memory {
                input_memory.add(estimate_batch(&batch))?;
            }
            for row in 0..batch.len {
                if !batch.selection.is_active(row, batch.len) {
                    continue;
                }
                let admitted = if simple_keys {
                    keys.clear();
                    keys.extend(key_indices.iter().map(|index| {
                        index.map_or(Value::Missing, |index| {
                            BatchToRowAdapter::extract_value(&batch.columns[index], row)
                        })
                    }));
                    top_n.try_push_lazy(&keys, || {
                        let values = batch.columns.iter_mut().map(|column| take_value(column, row)).collect();
                        Record::new(&self.schema.names, values)
                    })?
                } else {
                    // Preserve nested-path semantics until a column-native
                    // path evaluator is available for these less common keys.
                    let values = batch.columns.iter_mut().map(|column| take_value(column, row)).collect();
                    top_n.try_push(Record::new(&self.schema.names, values))?;
                    true
                };
                if track_memory && admitted {
                    self.memory.resize(top_n.estimated_bytes())?;
                }
            }
        }
        #[cfg(test)]
        {
            self.peak_retained_rows = top_n.peak_retained();
        }
        let records = top_n.finish();
        let total_rows = records.len();
        let num_cols = self.schema.names.len();
        let mut sorted_values = vec![Vec::with_capacity(total_rows); num_cols];
        for record in records {
            if record.to_variables().len() == num_cols {
                for (index, (_, value)) in record.into_tuples().into_iter().enumerate() {
                    sorted_values[index].push(value);
                }
            } else {
                // Record collapses duplicate aliases to their last value.
                // Reconstruct every declared column by name so duplicate
                // schema slots cannot become short or misaligned columns.
                let mut values = record.into_variables();
                for (index, name) in self.schema.names.iter().enumerate() {
                    let value = if self.schema.names[index + 1..].contains(name) {
                        values.get(name).cloned().unwrap_or(Value::Missing)
                    } else {
                        values.remove(name).unwrap_or(Value::Missing)
                    };
                    sorted_values[index].push(value);
                }
            }
        }
        self.emit_sorted_values(sorted_values, total_rows)?;
        Ok(())
    }

    fn emit_sorted_values(&mut self, sorted_values: Vec<Vec<Value>>, total_rows: usize) -> StreamResult<()> {
        let num_cols = self.schema.names.len();
        let mut values: Vec<_> = sorted_values.into_iter().map(Vec::into_iter).collect();
        let mut offset = 0;
        while offset < total_rows {
            let chunk_len = (total_rows - offset).min(BATCH_SIZE);
            let mut columns = Vec::with_capacity(num_cols);
            for column in values.iter_mut().take(num_cols) {
                let chunk: Vec<Value> = column.by_ref().take(chunk_len).collect();
                columns.push(TypedColumn::Mixed {
                    data: chunk,
                    null: Bitmap::all_set(chunk_len),
                    missing: Bitmap::all_set(chunk_len),
                });
            }
            self.result_batches.push(ColumnBatch {
                columns,
                names: self.schema.names.clone(),
                selection: SelectionVector::All,
                len: chunk_len,
            });
            offset += chunk_len;
        }
        if self.memory.is_enabled() {
            let output_bytes = self
                .result_batches
                .iter()
                .map(estimate_batch)
                .sum::<usize>()
                .saturating_add(self.result_batches.capacity() * std::mem::size_of::<ColumnBatch>());
            // ColumnBatch does not carry its own reservation. Keep the complete
            // result charged until this operator drops, including emitted batches.
            self.memory.resize(self.memory.bytes().max(output_bytes))?;
        }
        Ok(())
    }

    #[cfg(test)]
    fn peak_retained_rows(&self) -> usize {
        self.peak_retained_rows
    }
}

/// Mixed columns already own Values; moving them avoids re-cloning strings,
/// nested objects and arrays during gather. Other typed encodings materialize
/// only the selected row into a Value.
fn take_value(column: &mut TypedColumn, row: usize) -> Value {
    if let TypedColumn::Mixed { data, null, missing } = column {
        if !missing.is_set(row) {
            Value::Missing
        } else if !null.is_set(row) {
            Value::Null
        } else {
            std::mem::replace(&mut data[row], Value::Missing)
        }
    } else {
        BatchToRowAdapter::extract_value(column, row)
    }
}

impl BatchStream for BatchOrderByOperator {
    fn next_batch(&mut self) -> StreamResult<Option<ColumnBatch>> {
        if !self.consumed {
            self.consumed = true;
            self.consume_and_sort()?;
        }
        if self.emit_idx < self.result_batches.len() {
            let idx = self.emit_idx;
            self.emit_idx += 1;
            // Take the batch out — we won't need it again
            let batch = std::mem::replace(
                &mut self.result_batches[idx],
                ColumnBatch {
                    columns: Vec::new(),
                    names: Vec::new(),
                    selection: SelectionVector::All,
                    len: 0,
                },
            );
            Ok(Some(batch))
        } else {
            Ok(None)
        }
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
    use crate::simd::padded_vec::PaddedVec;
    use ordered_float::OrderedFloat;

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

    fn mixed_source(values: Vec<Value>) -> Box<dyn BatchStream> {
        let len = values.len();
        Box::new(OneBatch {
            batch: Some(ColumnBatch {
                columns: vec![TypedColumn::Mixed {
                    data: values,
                    null: Bitmap::all_set(len),
                    missing: Bitmap::all_set(len),
                }],
                names: vec!["x".into()],
                selection: SelectionVector::All,
                len,
            }),
            schema: BatchSchema {
                names: vec!["x".into()],
                types: vec![ColumnType::Mixed],
            },
        })
    }

    #[test]
    fn full_sort_and_top_k_preserve_numeric_order_and_nullish_ties() {
        let values = vec![
            Value::Null,
            Value::Int(3),
            Value::Float(OrderedFloat(1.5)),
            Value::Missing,
            Value::Float(OrderedFloat(-0.0)),
            Value::Int(0),
            Value::Int(16_777_217),
            Value::Float(OrderedFloat(16_777_216.0)),
        ];
        let expected = vec![
            Value::Float(OrderedFloat(-0.0)),
            Value::Int(0),
            Value::Float(OrderedFloat(1.5)),
            Value::Int(3),
            Value::Float(OrderedFloat(16_777_216.0)),
            Value::Int(16_777_217),
            Value::Null,
            Value::Missing,
        ];
        for limit in [None, Some(values.len())] {
            let mut op = BatchOrderByOperator::with_limit(
                mixed_source(values.clone()),
                vec![PathExpr::new(vec![PathSegment::AttrName("x".into())])],
                vec![Ordering::Asc],
                limit,
            );
            let batch = op.next_batch().unwrap().unwrap();
            let output: Vec<_> = (0..batch.len)
                .map(|row| BatchToRowAdapter::extract_value(&batch.columns[0], row))
                .collect();
            assert_eq!(output, expected, "limit {limit:?}");
        }
        assert_eq!(compare_values(&Value::Null, &Value::Missing), cmp::Ordering::Equal);
        assert_eq!(compare_values(&Value::Missing, &Value::Null), cmp::Ordering::Equal);
    }

    #[test]
    fn full_sort_moves_string_payloads_across_output_batches() {
        let values: Vec<Value> = (0..BATCH_SIZE + 3)
            .rev()
            .map(|i| Value::String(format!("{i:06}-{}", "payload".repeat(20)).into()))
            .collect();
        let original_pointers: std::collections::HashMap<String, *const u8> = values
            .iter()
            .map(|v| {
                let Value::String(s) = v else { unreachable!() };
                (s.to_string(), s.as_ptr())
            })
            .collect();
        let mut op = BatchOrderByOperator::new(
            mixed_source(values),
            vec![PathExpr::new(vec![PathSegment::AttrName("x".into())])],
            vec![Ordering::Asc],
        );
        let mut output = Vec::new();
        while let Some(batch) = op.next_batch().unwrap() {
            let TypedColumn::Mixed { data, .. } = batch.columns.into_iter().next().unwrap() else {
                unreachable!()
            };
            for value in data {
                let Value::String(s) = value else { unreachable!() };
                assert_eq!(
                    s.as_ptr(),
                    original_pointers[&s.to_string()],
                    "sort cloned a string payload"
                );
                output.push(s);
            }
        }
        assert_eq!(output.len(), BATCH_SIZE + 3);
        assert!(output.windows(2).all(|pair| pair[0] <= pair[1]));
    }

    #[test]
    fn both_sort_strategies_honor_a_shared_budget_and_release_on_drop() {
        use crate::execution::memory::{MemoryReservation, MemoryTracker};
        for limit in [None, Some(2)] {
            let tracker = MemoryTracker::new(Some(4096));
            let mut other = MemoryReservation::new(tracker.clone());
            other.add(4000).unwrap();
            let values = vec![
                Value::String("a".repeat(256).into()),
                Value::String("b".repeat(256).into()),
            ];
            let mut op = BatchOrderByOperator::with_limit(
                mixed_source(values),
                vec![PathExpr::new(vec![PathSegment::AttrName("x".into())])],
                vec![Ordering::Asc],
                limit,
            )
            .with_memory_tracker(tracker.clone());
            assert!(matches!(
                op.next_batch(),
                Err(crate::execution::types::StreamError::MemoryBudgetExceeded)
            ));
            drop(op);
            assert_eq!(tracker.used(), 4000);
            drop(other);
            assert_eq!(tracker.used(), 0);
        }
    }

    #[test]
    fn sorted_output_remains_conservatively_charged_until_operator_drop() {
        use crate::execution::memory::MemoryTracker;
        let tracker = MemoryTracker::new(Some(4096));
        let mut op = BatchOrderByOperator::new(
            mixed_source(vec![Value::Int(3), Value::Int(1)]),
            vec![PathExpr::new(vec![PathSegment::AttrName("x".into())])],
            vec![Ordering::Asc],
        )
        .with_memory_tracker(tracker.clone());
        let batch = op.next_batch().unwrap().unwrap();
        assert!(tracker.used() > 0);
        drop(batch);
        assert!(tracker.used() > 0);
        drop(op);
        assert_eq!(tracker.used(), 0);
    }

    #[test]
    fn test_order_by_int_asc() {
        let col = TypedColumn::Int32 {
            data: PaddedVec::from_vec(vec![30, 10, 20]),
            null: Bitmap::all_set(3),
            missing: Bitmap::all_set(3),
        };
        let batch = ColumnBatch {
            columns: vec![col],
            names: vec!["x".to_string()],
            selection: SelectionVector::All,
            len: 3,
        };
        let schema = BatchSchema {
            names: vec!["x".to_string()],
            types: vec![ColumnType::Int32],
        };

        let sort_col = PathExpr::new(vec![PathSegment::AttrName("x".to_string())]);
        let mut op = BatchOrderByOperator::new(
            Box::new(OneBatch {
                batch: Some(batch),
                schema,
            }),
            vec![sort_col],
            vec![Ordering::Asc],
        );

        let result = op.next_batch().unwrap().unwrap();
        assert_eq!(result.len, 3);
        let v0 = BatchToRowAdapter::extract_value(&result.columns[0], 0);
        let v1 = BatchToRowAdapter::extract_value(&result.columns[0], 1);
        let v2 = BatchToRowAdapter::extract_value(&result.columns[0], 2);
        assert_eq!(v0, Value::Int(10));
        assert_eq!(v1, Value::Int(20));
        assert_eq!(v2, Value::Int(30));

        assert!(op.next_batch().unwrap().is_none());
    }

    #[test]
    fn test_top_n_retains_only_the_requested_rows() {
        let values: Vec<i32> = (1..=100).rev().collect();
        let col = TypedColumn::Int32 {
            data: PaddedVec::from_vec(values),
            null: Bitmap::all_set(100),
            missing: Bitmap::all_set(100),
        };
        let batch = ColumnBatch {
            columns: vec![col],
            names: vec!["x".to_string()],
            selection: SelectionVector::All,
            len: 100,
        };
        let schema = BatchSchema {
            names: vec!["x".to_string()],
            types: vec![ColumnType::Int32],
        };
        let sort_col = PathExpr::new(vec![PathSegment::AttrName("x".to_string())]);
        let mut op = BatchOrderByOperator::new_top_n(
            Box::new(OneBatch {
                batch: Some(batch),
                schema,
            }),
            vec![sort_col],
            vec![Ordering::Asc],
            3,
        );

        let result = op.next_batch().unwrap().unwrap();
        assert_eq!(op.peak_retained_rows(), 3);
        assert_eq!(result.len, 3);
        let values: Vec<_> = (0..3)
            .map(|row| BatchToRowAdapter::extract_value(&result.columns[0], row))
            .collect();
        assert_eq!(values, vec![Value::Int(1), Value::Int(2), Value::Int(3)]);
    }

    #[test]
    fn test_order_by_int_desc() {
        let col = TypedColumn::Int32 {
            data: PaddedVec::from_vec(vec![30, 10, 20]),
            null: Bitmap::all_set(3),
            missing: Bitmap::all_set(3),
        };
        let batch = ColumnBatch {
            columns: vec![col],
            names: vec!["x".to_string()],
            selection: SelectionVector::All,
            len: 3,
        };
        let schema = BatchSchema {
            names: vec!["x".to_string()],
            types: vec![ColumnType::Int32],
        };

        let sort_col = PathExpr::new(vec![PathSegment::AttrName("x".to_string())]);
        let mut op = BatchOrderByOperator::new(
            Box::new(OneBatch {
                batch: Some(batch),
                schema,
            }),
            vec![sort_col],
            vec![Ordering::Desc],
        );

        let result = op.next_batch().unwrap().unwrap();
        let v0 = BatchToRowAdapter::extract_value(&result.columns[0], 0);
        let v1 = BatchToRowAdapter::extract_value(&result.columns[0], 1);
        let v2 = BatchToRowAdapter::extract_value(&result.columns[0], 2);
        assert_eq!(v0, Value::Int(30));
        assert_eq!(v1, Value::Int(20));
        assert_eq!(v2, Value::Int(10));
    }

    #[test]
    fn test_order_by_respects_selection() {
        let col = TypedColumn::Int32 {
            data: PaddedVec::from_vec(vec![30, 10, 20, 40]),
            null: Bitmap::all_set(4),
            missing: Bitmap::all_set(4),
        };
        let mut sel = Bitmap::all_unset(4);
        sel.set(0);
        sel.set(2);
        sel.set(3);
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

        let sort_col = PathExpr::new(vec![PathSegment::AttrName("x".to_string())]);
        let mut op = BatchOrderByOperator::new(
            Box::new(OneBatch {
                batch: Some(batch),
                schema,
            }),
            vec![sort_col],
            vec![Ordering::Asc],
        );

        let result = op.next_batch().unwrap().unwrap();
        assert_eq!(result.len, 3); // only 3 active rows
        let v0 = BatchToRowAdapter::extract_value(&result.columns[0], 0);
        let v1 = BatchToRowAdapter::extract_value(&result.columns[0], 1);
        let v2 = BatchToRowAdapter::extract_value(&result.columns[0], 2);
        assert_eq!(v0, Value::Int(20));
        assert_eq!(v1, Value::Int(30));
        assert_eq!(v2, Value::Int(40));
    }

    #[test]
    fn test_order_by_empty() {
        let schema = BatchSchema {
            names: vec!["x".to_string()],
            types: vec![ColumnType::Int32],
        };
        struct Empty {
            schema: BatchSchema,
        }
        impl BatchStream for Empty {
            fn next_batch(&mut self) -> StreamResult<Option<ColumnBatch>> {
                Ok(None)
            }
            fn schema(&self) -> &BatchSchema {
                &self.schema
            }
            fn close(&self) {}
        }

        let sort_col = PathExpr::new(vec![PathSegment::AttrName("x".to_string())]);
        let mut op = BatchOrderByOperator::new(Box::new(Empty { schema }), vec![sort_col], vec![Ordering::Asc]);

        assert!(op.next_batch().unwrap().is_none());
    }
}
