// src/execution/batch_distinct.rs

use crate::common::types::Value;
use crate::execution::batch::*;
use crate::execution::types::StreamResult;
use crate::simd::bitmap::Bitmap;
use crate::simd::selection::SelectionVector;
use hashbrown::HashSet;
use std::hash::{Hash, Hasher};

/// Batch-native DISTINCT operator. Tracks seen row keys and deselects
/// duplicate rows by updating the selection bitmap.
pub(crate) struct BatchDistinctOperator {
    child: Box<dyn BatchStream>,
    schema: BatchSchema,
    seen: HashSet<RowKey>,
}

/// A hashable row key built from extracted Values.
#[derive(Clone, Debug, PartialEq, Eq)]
struct RowKey(Vec<KeyValue>);

#[derive(Clone, Debug)]
enum KeyValue {
    Int(i32),
    Float(ordered_float::OrderedFloat<f32>),
    Bool(bool),
    Str(String),
    DateTime(i64),
    Null,
    Missing,
}

impl PartialEq for KeyValue {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (KeyValue::Int(a), KeyValue::Int(b)) => a == b,
            (KeyValue::Float(a), KeyValue::Float(b)) => a == b,
            (KeyValue::Bool(a), KeyValue::Bool(b)) => a == b,
            (KeyValue::Str(a), KeyValue::Str(b)) => a == b,
            (KeyValue::DateTime(a), KeyValue::DateTime(b)) => a == b,
            (KeyValue::Null, KeyValue::Null) => true,
            (KeyValue::Missing, KeyValue::Missing) => true,
            _ => false,
        }
    }
}

impl Eq for KeyValue {}

impl Hash for KeyValue {
    fn hash<H: Hasher>(&self, state: &mut H) {
        std::mem::discriminant(self).hash(state);
        match self {
            KeyValue::Int(v) => v.hash(state),
            KeyValue::Float(v) => v.hash(state),
            KeyValue::Bool(v) => v.hash(state),
            KeyValue::Str(v) => v.hash(state),
            KeyValue::DateTime(v) => v.hash(state),
            KeyValue::Null | KeyValue::Missing => {}
        }
    }
}

impl Hash for RowKey {
    fn hash<H: Hasher>(&self, state: &mut H) {
        for v in &self.0 {
            v.hash(state);
        }
    }
}

fn value_to_key(val: &Value) -> KeyValue {
    match val {
        Value::Int(v) => KeyValue::Int(*v),
        Value::Float(v) => KeyValue::Float(*v),
        Value::Boolean(v) => KeyValue::Bool(*v),
        Value::String(v) => KeyValue::Str(v.to_string()),
        Value::DateTime(v) => KeyValue::DateTime(v.timestamp()),
        Value::Null => KeyValue::Null,
        Value::Missing => KeyValue::Missing,
        // For complex types, use debug string as key
        other => KeyValue::Str(format!("{:?}", other)),
    }
}

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
        }
    }
}

impl BatchStream for BatchDistinctOperator {
    fn next_batch(&mut self) -> StreamResult<Option<ColumnBatch>> {
        while let Some(batch) = self.child.next_batch()? {
            let mut active_count = 0;
            let mut sel_bytes = vec![0u8; batch.len];

            for row in 0..batch.len {
                if !batch.selection.is_active(row, batch.len) {
                    continue;
                }
                // Build row key from all columns
                let key_vals: Vec<KeyValue> = batch.columns.iter().map(|col| {
                    let val = BatchToRowAdapter::extract_value(col, row);
                    value_to_key(&val)
                }).collect();
                let key = RowKey(key_vals);

                if self.seen.insert(key) {
                    sel_bytes[row] = 1;
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
