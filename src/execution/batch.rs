// src/execution/batch.rs

use crate::common::types::Value;
use crate::execution::stream::{Record, RecordStream};
use crate::execution::types::StreamResult;
use crate::simd::bitmap::Bitmap;
use crate::simd::padded_vec::PaddedVec;
use crate::simd::selection::SelectionVector;
use linked_hash_map::LinkedHashMap;
use ordered_float::OrderedFloat;

/// Compile-time tunable batch size.
pub const BATCH_SIZE: usize = 1024;

/// A typed columnar array with NULL and MISSING tracking.
///
/// Each variant carries two bitmaps encoding PartiQL's three-valued logic:
/// - `missing` bit 0: value is MISSING (`null` bit and data are don't-care)
/// - `missing` bit 1, `null` bit 0: value is NULL
/// - `missing` bit 1, `null` bit 1: value is present (read from `data`)
pub enum TypedColumn {
    Int32 {
        data: PaddedVec<i32>,
        null: Bitmap,
        missing: Bitmap,
    },
    Float32 {
        data: PaddedVec<f32>,
        null: Bitmap,
        missing: Bitmap,
    },
    Boolean {
        data: Bitmap,
        null: Bitmap,
        missing: Bitmap,
    },
    Utf8 {
        data: PaddedVec<u8>,
        offsets: PaddedVec<u32>,
        null: Bitmap,
        missing: Bitmap,
    },
    DateTime {
        data: PaddedVec<i64>,
        null: Bitmap,
        missing: Bitmap,
    },
    Mixed {
        data: Vec<Value>,
        null: Bitmap,
        missing: Bitmap,
    },
}

/// A batch of up to BATCH_SIZE rows in columnar layout.
pub struct ColumnBatch {
    pub columns: Vec<TypedColumn>,
    pub names: Vec<String>,
    pub selection: SelectionVector,
    pub len: usize,
}

/// Schema information for a batch.
pub struct BatchSchema {
    pub names: Vec<String>,
    pub types: Vec<ColumnType>,
}

/// Column type tag (without data).
#[derive(Clone, Debug, PartialEq)]
pub enum ColumnType {
    Int32,
    Float32,
    Boolean,
    Utf8,
    DateTime,
    Mixed,
}

/// Batch-oriented stream trait. Replaces RecordStream for SIMD operators.
pub trait BatchStream {
    fn next_batch(&mut self) -> StreamResult<Option<ColumnBatch>>;
    fn schema(&self) -> &BatchSchema;
    fn close(&self);
}

/// Converts a [`BatchStream`] into a [`RecordStream`] by materializing each
/// columnar batch into individual [`Record`] rows, respecting the
/// [`SelectionVector`] (skipping inactive rows).
pub struct BatchToRowAdapter {
    source: Box<dyn BatchStream>,
    current_batch: Option<ColumnBatch>,
    row_idx: usize,
}

impl BatchToRowAdapter {
    pub fn new(source: Box<dyn BatchStream>) -> Self {
        BatchToRowAdapter {
            source,
            current_batch: None,
            row_idx: 0,
        }
    }

    /// Extracts a single [`Value`] from a [`TypedColumn`] at the given row index.
    ///
    /// Checks `missing` bitmap first (if false -> `Value::Missing`), then
    /// `null` bitmap (if false -> `Value::Null`), then reads the actual data.
    pub(crate) fn extract_value(col: &TypedColumn, row: usize) -> Value {
        match col {
            TypedColumn::Int32 { data, null, missing } => {
                if !missing.is_set(row) {
                    return Value::Missing;
                }
                if !null.is_set(row) {
                    return Value::Null;
                }
                Value::Int(data[row])
            }
            TypedColumn::Float32 { data, null, missing } => {
                if !missing.is_set(row) {
                    return Value::Missing;
                }
                if !null.is_set(row) {
                    return Value::Null;
                }
                Value::Float(OrderedFloat(data[row]))
            }
            TypedColumn::Boolean { data, null, missing } => {
                if !missing.is_set(row) {
                    return Value::Missing;
                }
                if !null.is_set(row) {
                    return Value::Null;
                }
                Value::Boolean(data.is_set(row))
            }
            TypedColumn::Utf8 { data, offsets, null, missing } => {
                if !missing.is_set(row) {
                    return Value::Missing;
                }
                if !null.is_set(row) {
                    return Value::Null;
                }
                let start = offsets[row] as usize;
                let end = offsets[row + 1] as usize;
                let s = String::from_utf8_lossy(&data[start..end]).into_owned();
                Value::String(s)
            }
            TypedColumn::DateTime { data, null, missing } => {
                if !missing.is_set(row) {
                    return Value::Missing;
                }
                if !null.is_set(row) {
                    return Value::Null;
                }
                let micros = data[row];
                let secs = micros / 1_000_000;
                let nanos = ((micros % 1_000_000) * 1000) as u32;
                let naive = chrono::NaiveDateTime::from_timestamp_opt(secs, nanos)
                    .expect("invalid timestamp");
                let fixed = chrono::DateTime::<chrono::FixedOffset>::from_utc(
                    naive,
                    chrono::FixedOffset::east(0),
                );
                Value::DateTime(fixed)
            }
            TypedColumn::Mixed { data, null, missing } => {
                if !missing.is_set(row) {
                    return Value::Missing;
                }
                if !null.is_set(row) {
                    return Value::Null;
                }
                data[row].clone()
            }
        }
    }
}

impl RecordStream for BatchToRowAdapter {
    fn next(&mut self) -> StreamResult<Option<Record>> {
        loop {
            // If we have no current batch, fetch the next one
            if self.current_batch.is_none() {
                match self.source.next_batch()? {
                    Some(batch) => {
                        self.current_batch = Some(batch);
                        self.row_idx = 0;
                    }
                    None => return Ok(None),
                }
            }

            let batch = self.current_batch.as_ref().unwrap();

            // Scan forward through remaining rows in the batch
            while self.row_idx < batch.len {
                let row = self.row_idx;
                self.row_idx += 1;

                // Skip inactive rows per the selection vector
                if !batch.selection.is_active(row, batch.len) {
                    continue;
                }

                // Build the Variables map from each column
                let mut variables = LinkedHashMap::with_capacity(batch.columns.len());
                for (col_idx, col) in batch.columns.iter().enumerate() {
                    let value = Self::extract_value(col, row);
                    variables.insert(batch.names[col_idx].clone(), value);
                }

                return Ok(Some(Record::new_with_variables(variables)));
            }

            // Current batch exhausted; drop it and try next
            self.current_batch = None;
        }
    }

    fn close(&self) {
        self.source.close();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::simd::bitmap::Bitmap;
    use crate::simd::padded_vec::{PaddedVec, PaddedVecBuilder};

    #[test]
    fn test_typed_column_int32_construction() {
        let data = PaddedVec::from_vec(vec![1i32, 2, 3]);
        let null = Bitmap::all_set(3);
        let missing = Bitmap::all_set(3);
        let col = TypedColumn::Int32 { data, null, missing };
        match &col {
            TypedColumn::Int32 { data, .. } => assert_eq!(data.len(), 3),
            _ => panic!("expected Int32"),
        }
    }

    #[test]
    fn test_typed_column_utf8_builder() {
        let mut data_builder = PaddedVecBuilder::<u8>::new();
        let mut offsets_builder = PaddedVecBuilder::<u32>::with_capacity(4);
        offsets_builder.push(0);
        data_builder.extend_from_slice(b"hello");
        offsets_builder.push(data_builder.len() as u32);
        data_builder.extend_from_slice(b"world");
        offsets_builder.push(data_builder.len() as u32);

        let data = data_builder.seal();
        let offsets = offsets_builder.seal();
        assert_eq!(&*data, b"helloworld");
        assert_eq!(&*offsets, &[0u32, 5, 10]);
    }

    #[test]
    fn test_batch_stream_trait_is_object_safe() {
        // Verify trait can be used as dyn
        fn _takes_stream(_s: &dyn BatchStream) {}
    }

    struct EmptyBatchStream {
        schema: BatchSchema,
    }

    impl BatchStream for EmptyBatchStream {
        fn next_batch(&mut self) -> crate::execution::types::StreamResult<Option<ColumnBatch>> {
            Ok(None)
        }
        fn schema(&self) -> &BatchSchema {
            &self.schema
        }
        fn close(&self) {}
    }

    #[test]
    fn test_empty_batch_stream() {
        let mut stream = EmptyBatchStream {
            schema: BatchSchema { names: vec![], types: vec![] },
        };
        assert!(stream.next_batch().unwrap().is_none());
    }

    #[test]
    fn test_column_batch_construction() {
        let col = TypedColumn::Int32 {
            data: PaddedVec::from_vec(vec![10, 20]),
            null: Bitmap::all_set(2),
            missing: Bitmap::all_set(2),
        };
        let batch = ColumnBatch {
            columns: vec![col],
            names: vec!["x".to_string()],
            selection: crate::simd::selection::SelectionVector::All,
            len: 2,
        };
        assert_eq!(batch.len, 2);
        assert_eq!(batch.names.len(), 1);
    }

    #[test]
    fn test_batch_to_row_adapter_empty() {
        use crate::execution::stream::RecordStream;
        let schema = BatchSchema { names: vec![], types: vec![] };
        struct NoBatches { schema: BatchSchema }
        impl BatchStream for NoBatches {
            fn next_batch(&mut self) -> crate::execution::types::StreamResult<Option<ColumnBatch>> { Ok(None) }
            fn schema(&self) -> &BatchSchema { &self.schema }
            fn close(&self) {}
        }
        let mut adapter = BatchToRowAdapter::new(Box::new(NoBatches { schema }));
        assert!(adapter.next().unwrap().is_none());
    }

    #[test]
    fn test_batch_to_row_adapter_converts_int32() {
        use crate::execution::stream::RecordStream;
        let col = TypedColumn::Int32 {
            data: PaddedVec::from_vec(vec![10, 20, 30]),
            null: Bitmap::all_set(3),
            missing: Bitmap::all_set(3),
        };
        let batch = ColumnBatch {
            columns: vec![col],
            names: vec!["x".to_string()],
            selection: crate::simd::selection::SelectionVector::All,
            len: 3,
        };
        let schema = BatchSchema { names: vec!["x".to_string()], types: vec![ColumnType::Int32] };
        struct OneBatch { batch: Option<ColumnBatch>, schema: BatchSchema }
        impl BatchStream for OneBatch {
            fn next_batch(&mut self) -> crate::execution::types::StreamResult<Option<ColumnBatch>> { Ok(self.batch.take()) }
            fn schema(&self) -> &BatchSchema { &self.schema }
            fn close(&self) {}
        }
        let mut adapter = BatchToRowAdapter::new(Box::new(OneBatch { batch: Some(batch), schema }));
        let r1 = adapter.next().unwrap().unwrap();
        assert_eq!(r1.to_variables()["x"], crate::common::types::Value::Int(10));
        let r2 = adapter.next().unwrap().unwrap();
        assert_eq!(r2.to_variables()["x"], crate::common::types::Value::Int(20));
        let r3 = adapter.next().unwrap().unwrap();
        assert_eq!(r3.to_variables()["x"], crate::common::types::Value::Int(30));
        assert!(adapter.next().unwrap().is_none());
    }

    #[test]
    fn test_batch_to_row_adapter_respects_selection() {
        use crate::execution::stream::RecordStream;
        let col = TypedColumn::Int32 {
            data: PaddedVec::from_vec(vec![10, 20, 30]),
            null: Bitmap::all_set(3),
            missing: Bitmap::all_set(3),
        };
        let mut sel = Bitmap::all_unset(3);
        sel.set(0); sel.set(2);
        let batch = ColumnBatch {
            columns: vec![col],
            names: vec!["x".to_string()],
            selection: crate::simd::selection::SelectionVector::Bitmap(sel),
            len: 3,
        };
        let schema = BatchSchema { names: vec!["x".to_string()], types: vec![ColumnType::Int32] };
        struct OneBatch { batch: Option<ColumnBatch>, schema: BatchSchema }
        impl BatchStream for OneBatch {
            fn next_batch(&mut self) -> crate::execution::types::StreamResult<Option<ColumnBatch>> { Ok(self.batch.take()) }
            fn schema(&self) -> &BatchSchema { &self.schema }
            fn close(&self) {}
        }
        let mut adapter = BatchToRowAdapter::new(Box::new(OneBatch { batch: Some(batch), schema }));
        let r1 = adapter.next().unwrap().unwrap();
        assert_eq!(r1.to_variables()["x"], crate::common::types::Value::Int(10));
        let r2 = adapter.next().unwrap().unwrap();
        assert_eq!(r2.to_variables()["x"], crate::common::types::Value::Int(30));
        assert!(adapter.next().unwrap().is_none());
    }
}
