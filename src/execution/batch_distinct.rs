// src/execution/batch_distinct.rs

use crate::common::types::Value;
use crate::execution::batch::*;
use crate::execution::memory::{MemoryReservation, MemoryTracker, estimate_batch, estimate_value};
use crate::execution::types::StreamResult;
use crate::simd::bitmap::Bitmap;
use crate::simd::selection::SelectionVector;
use hashbrown::HashSet;

/// Batch-native DISTINCT operator. Tracks seen row keys and deselects
/// duplicate rows by updating the selection bitmap.
pub(crate) struct BatchDistinctOperator {
    child: Box<dyn BatchStream>,
    schema: BatchSchema,
    seen: HashSet<RowKey>,
    memory: MemoryReservation,
}

/// Keep the same typed equality and hashing as the row pipeline. Datetime
/// precision and complex values must not collapse into string representations.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct RowKey(Vec<Value>);

impl BatchDistinctOperator {
    pub fn new(child: Box<dyn BatchStream>) -> Self {
        let schema = BatchSchema {
            names: child.schema().names.clone(),
            types: child.schema().types.clone(),
        };
        Self {
            child,
            schema,
            seen: HashSet::new(),
            memory: MemoryReservation::default(),
        }
    }

    pub(crate) fn with_memory_tracker(mut self, memory: MemoryTracker) -> Self {
        self.memory = MemoryReservation::new(memory);
        self
    }
}

impl BatchStream for BatchDistinctOperator {
    fn next_batch(&mut self) -> StreamResult<Option<ColumnBatch>> {
        while let Some(batch) = self.child.next_batch()? {
            let mut active_count = 0;
            let mut sel_bytes = vec![0u8; batch.len];

            for (row, selected) in sel_bytes.iter_mut().enumerate().take(batch.len) {
                if !batch.selection.is_active(row, batch.len) {
                    continue;
                }
                // Build row key from all columns
                let key_vals: Vec<Value> = batch
                    .columns
                    .iter()
                    .map(|col| BatchToRowAdapter::extract_value(col, row))
                    .collect();
                let key = RowKey(key_vals);

                if !self.seen.contains(&key) {
                    if self.memory.is_enabled() {
                        let key_bytes = 64usize
                            .saturating_add(key.0.capacity() * std::mem::size_of::<Value>())
                            .saturating_add(key.0.iter().map(estimate_value).sum::<usize>());
                        self.memory.add(key_bytes)?;
                    }
                    self.seen.insert(key);
                    *selected = 1;
                    active_count += 1;
                }
            }

            if active_count == 0 {
                continue; // All rows were duplicates, skip this batch
            }

            let sel_bitmap = Bitmap::pack_from_bytes(&sel_bytes);
            let result = ColumnBatch {
                columns: batch.columns,
                names: batch.names,
                selection: SelectionVector::Bitmap(sel_bitmap),
                len: batch.len,
            };
            if self.memory.is_enabled() {
                // Keep emitted storage conservatively charged along with the
                // keys, since ColumnBatch has no reservation of its own.
                self.memory.add(estimate_batch(&result))?;
            }
            return Ok(Some(result));
        }
        Ok(None)
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

    struct MultiBatch {
        batches: Vec<ColumnBatch>,
        idx: usize,
        schema: BatchSchema,
    }

    impl BatchStream for MultiBatch {
        fn next_batch(&mut self) -> StreamResult<Option<ColumnBatch>> {
            if self.idx < self.batches.len() {
                let i = self.idx;
                self.idx += 1;
                let batch = std::mem::replace(
                    &mut self.batches[i],
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
        fn close(&self) {}
    }

    fn mixed_source(values: Vec<Value>) -> Box<dyn BatchStream> {
        let len = values.len();
        Box::new(MultiBatch {
            batches: vec![ColumnBatch {
                columns: vec![TypedColumn::Mixed {
                    data: values,
                    null: Bitmap::all_set(len),
                    missing: Bitmap::all_set(len),
                }],
                names: vec!["x".into()],
                selection: SelectionVector::All,
                len,
            }],
            idx: 0,
            schema: BatchSchema {
                names: vec!["x".into()],
                types: vec![ColumnType::Mixed],
            },
        })
    }

    #[test]
    fn distinct_preserves_datetime_precision_and_complex_value_types() {
        let first = Value::DateTime(chrono::DateTime::parse_from_rfc3339("2026-01-01T00:00:00.000000001Z").unwrap());
        let second = Value::DateTime(chrono::DateTime::parse_from_rfc3339("2026-01-01T00:00:00.000000002Z").unwrap());
        let array = Value::Array(vec![Value::Int(1)]);
        let string = Value::String(format!("{array:?}").into());
        let mut op = BatchDistinctOperator::new(mixed_source(vec![
            first.clone(),
            second,
            first,
            array.clone(),
            string,
            array,
        ]));
        let batch = op.next_batch().unwrap().unwrap();
        assert_eq!(batch.selection.count_active(batch.len), 4);
    }

    #[test]
    fn distinct_charges_unique_keys_and_releases_its_share_on_failure() {
        use crate::execution::memory::{MemoryReservation, MemoryTracker};
        let tracker = MemoryTracker::new(Some(4096));
        let mut other = MemoryReservation::new(tracker.clone());
        other.add(4000).unwrap();
        let mut op = BatchDistinctOperator::new(mixed_source(vec![Value::String("x".repeat(256).into())]))
            .with_memory_tracker(tracker.clone());
        assert!(matches!(
            op.next_batch(),
            Err(crate::execution::types::StreamError::MemoryBudgetExceeded)
        ));
        drop(op);
        assert_eq!(tracker.used(), 4000);
    }

    #[test]
    fn duplicate_only_batches_do_not_accumulate_more_budget() {
        use crate::execution::memory::MemoryTracker;
        let mut source = mixed_source(vec![Value::Int(1)]);
        let first = source.next_batch().unwrap().unwrap();
        let mut source = mixed_source(vec![Value::Int(1); 200]);
        let repeated = source.next_batch().unwrap().unwrap();
        let tracker = MemoryTracker::new(Some(4096));
        let mut op = BatchDistinctOperator::new(Box::new(MultiBatch {
            batches: vec![first, repeated],
            idx: 0,
            schema: source.schema().clone(),
        }))
        .with_memory_tracker(tracker.clone());
        let output = op.next_batch().unwrap().unwrap();
        let used = tracker.used();
        assert!(used > 0);
        drop(output);
        assert!(op.next_batch().unwrap().is_none());
        assert_eq!(tracker.used(), used);
        drop(op);
        assert_eq!(tracker.used(), 0);
    }

    #[test]
    fn test_distinct_removes_duplicates() {
        // [10, 20, 10, 30, 20]
        let col = TypedColumn::Int32 {
            data: PaddedVec::from_vec(vec![10, 20, 10, 30, 20]),
            null: Bitmap::all_set(5),
            missing: Bitmap::all_set(5),
        };
        let batch = ColumnBatch {
            columns: vec![col],
            names: vec!["x".to_string()],
            selection: SelectionVector::All,
            len: 5,
        };
        let schema = BatchSchema {
            names: vec!["x".to_string()],
            types: vec![ColumnType::Int32],
        };

        let mut op = BatchDistinctOperator::new(Box::new(MultiBatch {
            batches: vec![batch],
            idx: 0,
            schema,
        }));

        let result = op.next_batch().unwrap().unwrap();
        // Should have 3 unique rows: 10, 20, 30
        let mut values = Vec::new();
        for row in 0..result.len {
            if result.selection.is_active(row, result.len) {
                values.push(BatchToRowAdapter::extract_value(&result.columns[0], row));
            }
        }
        assert_eq!(values.len(), 3);
        assert_eq!(values[0], Value::Int(10));
        assert_eq!(values[1], Value::Int(20));
        assert_eq!(values[2], Value::Int(30));
    }

    #[test]
    fn test_distinct_across_batches() {
        let col1 = TypedColumn::Int32 {
            data: PaddedVec::from_vec(vec![10, 20]),
            null: Bitmap::all_set(2),
            missing: Bitmap::all_set(2),
        };
        let batch1 = ColumnBatch {
            columns: vec![col1],
            names: vec!["x".to_string()],
            selection: SelectionVector::All,
            len: 2,
        };

        let col2 = TypedColumn::Int32 {
            data: PaddedVec::from_vec(vec![20, 30]),
            null: Bitmap::all_set(2),
            missing: Bitmap::all_set(2),
        };
        let batch2 = ColumnBatch {
            columns: vec![col2],
            names: vec!["x".to_string()],
            selection: SelectionVector::All,
            len: 2,
        };

        let schema = BatchSchema {
            names: vec!["x".to_string()],
            types: vec![ColumnType::Int32],
        };

        let mut op = BatchDistinctOperator::new(Box::new(MultiBatch {
            batches: vec![batch1, batch2],
            idx: 0,
            schema,
        }));

        // First batch: both 10, 20 are new
        let r1 = op.next_batch().unwrap().unwrap();
        let mut v1 = Vec::new();
        for row in 0..r1.len {
            if r1.selection.is_active(row, r1.len) {
                v1.push(BatchToRowAdapter::extract_value(&r1.columns[0], row));
            }
        }
        assert_eq!(v1, vec![Value::Int(10), Value::Int(20)]);

        // Second batch: 20 is duplicate, only 30 is new
        let r2 = op.next_batch().unwrap().unwrap();
        let mut v2 = Vec::new();
        for row in 0..r2.len {
            if r2.selection.is_active(row, r2.len) {
                v2.push(BatchToRowAdapter::extract_value(&r2.columns[0], row));
            }
        }
        assert_eq!(v2, vec![Value::Int(30)]);

        assert!(op.next_batch().unwrap().is_none());
    }

    #[test]
    fn test_distinct_all_duplicates_skips_batch() {
        let col1 = TypedColumn::Int32 {
            data: PaddedVec::from_vec(vec![10, 20]),
            null: Bitmap::all_set(2),
            missing: Bitmap::all_set(2),
        };
        let batch1 = ColumnBatch {
            columns: vec![col1],
            names: vec!["x".to_string()],
            selection: SelectionVector::All,
            len: 2,
        };

        // Second batch has only duplicates
        let col2 = TypedColumn::Int32 {
            data: PaddedVec::from_vec(vec![10, 20]),
            null: Bitmap::all_set(2),
            missing: Bitmap::all_set(2),
        };
        let batch2 = ColumnBatch {
            columns: vec![col2],
            names: vec!["x".to_string()],
            selection: SelectionVector::All,
            len: 2,
        };

        // Third batch has one new value
        let col3 = TypedColumn::Int32 {
            data: PaddedVec::from_vec(vec![10, 30]),
            null: Bitmap::all_set(2),
            missing: Bitmap::all_set(2),
        };
        let batch3 = ColumnBatch {
            columns: vec![col3],
            names: vec!["x".to_string()],
            selection: SelectionVector::All,
            len: 2,
        };

        let schema = BatchSchema {
            names: vec!["x".to_string()],
            types: vec![ColumnType::Int32],
        };

        let mut op = BatchDistinctOperator::new(Box::new(MultiBatch {
            batches: vec![batch1, batch2, batch3],
            idx: 0,
            schema,
        }));

        let r1 = op.next_batch().unwrap().unwrap();
        assert_eq!(r1.len, 2); // both active

        // batch2 is all duplicates — should be skipped, returning batch3
        let r3 = op.next_batch().unwrap().unwrap();
        let mut v3 = Vec::new();
        for row in 0..r3.len {
            if r3.selection.is_active(row, r3.len) {
                v3.push(BatchToRowAdapter::extract_value(&r3.columns[0], row));
            }
        }
        assert_eq!(v3, vec![Value::Int(30)]);

        assert!(op.next_batch().unwrap().is_none());
    }

    #[test]
    fn test_distinct_empty() {
        let schema = BatchSchema {
            names: vec!["x".to_string()],
            types: vec![ColumnType::Int32],
        };

        let mut op = BatchDistinctOperator::new(Box::new(MultiBatch {
            batches: vec![],
            idx: 0,
            schema,
        }));

        assert!(op.next_batch().unwrap().is_none());
    }
}
