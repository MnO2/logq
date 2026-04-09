// src/execution/batch.rs

use std::collections::VecDeque;
use crate::common::types::Value;
use crate::execution::stream::{Record, RecordStream};
use crate::execution::types::StreamResult;
use crate::simd::bitmap::Bitmap;
use crate::simd::padded_vec::{PaddedVec, PaddedVecBuilder};
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
    /// Dictionary-encoded strings.  Stores each unique string once in
    /// `dict_data`/`dict_offsets` and a per-row `codes` array that indexes
    /// into the dictionary.  Used automatically when cardinality is low.
    DictUtf8 {
        dict_data: PaddedVec<u8>,
        dict_offsets: PaddedVec<u32>, // len = num_unique + 1
        codes: PaddedVec<u16>,
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

impl TypedColumn {
    /// Returns a bitmap where bit i is set iff row i is present (non-null and non-missing).
    pub fn validity_bitmap(&self, _len: usize) -> crate::simd::bitmap::Bitmap {
        let (null, missing) = match self {
            TypedColumn::Int32 { null, missing, .. } => (null, missing),
            TypedColumn::Float32 { null, missing, .. } => (null, missing),
            TypedColumn::Boolean { null, missing, .. } => (null, missing),
            TypedColumn::Utf8 { null, missing, .. } => (null, missing),
            TypedColumn::DictUtf8 { null, missing, .. } => (null, missing),
            TypedColumn::DateTime { null, missing, .. } => (null, missing),
            TypedColumn::Mixed { null, missing, .. } => (null, missing),
        };
        null.and(missing)
    }

    /// Returns true when all rows have present (non-null, non-missing) values.
    /// When true, predicate evaluation can skip null/missing bitmap checks.
    #[inline]
    pub fn all_present(&self, len: usize) -> bool {
        let (null, missing) = match self {
            TypedColumn::Int32 { null, missing, .. } => (null, missing),
            TypedColumn::Float32 { null, missing, .. } => (null, missing),
            TypedColumn::Boolean { null, missing, .. } => (null, missing),
            TypedColumn::Utf8 { null, missing, .. } => (null, missing),
            TypedColumn::DictUtf8 { null, missing, .. } => (null, missing),
            TypedColumn::DateTime { null, missing, .. } => (null, missing),
            TypedColumn::Mixed { null, missing, .. } => (null, missing),
        };
        null.count_ones() == len && missing.count_ones() == len
    }
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

/// A [`BatchStream`] backed by pre-computed batches (e.g. from parallel scanning).
/// Yields batches one at a time from an internal queue.
pub struct PrecomputedBatchStream {
    batches: VecDeque<ColumnBatch>,
    batch_schema: BatchSchema,
}

impl PrecomputedBatchStream {
    pub fn new(batches: Vec<ColumnBatch>, batch_schema: BatchSchema) -> Self {
        Self {
            batches: batches.into(),
            batch_schema,
        }
    }
}

impl BatchStream for PrecomputedBatchStream {
    fn next_batch(&mut self) -> StreamResult<Option<ColumnBatch>> {
        Ok(self.batches.pop_front())
    }

    fn schema(&self) -> &BatchSchema {
        &self.batch_schema
    }

    fn close(&self) {}
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
                Value::String(s.into())
            }
            TypedColumn::DictUtf8 { dict_data, dict_offsets, codes, null, missing } => {
                if !missing.is_set(row) {
                    return Value::Missing;
                }
                if !null.is_set(row) {
                    return Value::Null;
                }
                let code = codes[row] as usize;
                let start = dict_offsets[code] as usize;
                let end = dict_offsets[code + 1] as usize;
                let s = String::from_utf8_lossy(&dict_data[start..end]).into_owned();
                Value::String(s.into())
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

/// Converts a [`RecordStream`] into a [`BatchStream`] by buffering up to
/// [`BATCH_SIZE`] rows and packing them into columnar [`ColumnBatch`]es.
pub struct RowToBatchAdapter {
    source: Box<dyn RecordStream>,
    schema: BatchSchema,
    done: bool,
}

impl RowToBatchAdapter {
    pub fn new(source: Box<dyn RecordStream>, schema: BatchSchema) -> Self {
        RowToBatchAdapter {
            source,
            schema,
            done: false,
        }
    }
}

impl BatchStream for RowToBatchAdapter {
    fn next_batch(&mut self) -> StreamResult<Option<ColumnBatch>> {
        if self.done {
            return Ok(None);
        }

        // Collect up to BATCH_SIZE records from the source.
        let mut records = Vec::with_capacity(BATCH_SIZE);
        for _ in 0..BATCH_SIZE {
            match self.source.next()? {
                Some(record) => records.push(record),
                None => {
                    self.done = true;
                    break;
                }
            }
        }

        if records.is_empty() {
            return Ok(None);
        }

        let num_rows = records.len();
        let num_cols = self.schema.types.len();
        let mut columns = Vec::with_capacity(num_cols);

        for col_idx in 0..num_cols {
            let col_name = &self.schema.names[col_idx];
            let col_type = &self.schema.types[col_idx];

            let column = match col_type {
                ColumnType::Int32 => {
                    let mut data = Vec::with_capacity(num_rows);
                    let mut null_bm = Bitmap::all_set(num_rows);
                    let mut missing_bm = Bitmap::all_set(num_rows);
                    for (row_idx, record) in records.iter().enumerate() {
                        match record.to_variables().get(col_name) {
                            Some(Value::Int(v)) => data.push(*v),
                            Some(Value::Null) => {
                                data.push(0);
                                null_bm.unset(row_idx);
                            }
                            Some(Value::Missing) | None => {
                                data.push(0);
                                missing_bm.unset(row_idx);
                            }
                            _ => {
                                // Type mismatch — treat as null
                                data.push(0);
                                null_bm.unset(row_idx);
                            }
                        }
                    }
                    TypedColumn::Int32 {
                        data: PaddedVec::from_vec(data),
                        null: null_bm,
                        missing: missing_bm,
                    }
                }
                ColumnType::Float32 => {
                    let mut data = Vec::with_capacity(num_rows);
                    let mut null_bm = Bitmap::all_set(num_rows);
                    let mut missing_bm = Bitmap::all_set(num_rows);
                    for (row_idx, record) in records.iter().enumerate() {
                        match record.to_variables().get(col_name) {
                            Some(Value::Float(v)) => data.push(v.into_inner()),
                            Some(Value::Null) => {
                                data.push(0.0);
                                null_bm.unset(row_idx);
                            }
                            Some(Value::Missing) | None => {
                                data.push(0.0);
                                missing_bm.unset(row_idx);
                            }
                            _ => {
                                data.push(0.0);
                                null_bm.unset(row_idx);
                            }
                        }
                    }
                    TypedColumn::Float32 {
                        data: PaddedVec::from_vec(data),
                        null: null_bm,
                        missing: missing_bm,
                    }
                }
                ColumnType::Utf8 => {
                    let mut data_builder = PaddedVecBuilder::<u8>::new();
                    let mut offsets_builder =
                        PaddedVecBuilder::<u32>::with_capacity(num_rows + 1);
                    offsets_builder.push(0);
                    let mut null_bm = Bitmap::all_set(num_rows);
                    let mut missing_bm = Bitmap::all_set(num_rows);
                    for (row_idx, record) in records.iter().enumerate() {
                        match record.to_variables().get(col_name) {
                            Some(Value::String(s)) => {
                                data_builder.extend_from_slice(s.as_bytes());
                                offsets_builder.push(data_builder.len() as u32);
                            }
                            Some(Value::Null) => {
                                null_bm.unset(row_idx);
                                offsets_builder.push(data_builder.len() as u32);
                            }
                            Some(Value::Missing) | None => {
                                missing_bm.unset(row_idx);
                                offsets_builder.push(data_builder.len() as u32);
                            }
                            _ => {
                                null_bm.unset(row_idx);
                                offsets_builder.push(data_builder.len() as u32);
                            }
                        }
                    }
                    TypedColumn::Utf8 {
                        data: data_builder.seal(),
                        offsets: offsets_builder.seal(),
                        null: null_bm,
                        missing: missing_bm,
                    }
                }
                ColumnType::Boolean | ColumnType::DateTime | ColumnType::Mixed => {
                    let mut data = Vec::with_capacity(num_rows);
                    let mut null_bm = Bitmap::all_set(num_rows);
                    let mut missing_bm = Bitmap::all_set(num_rows);
                    for (row_idx, record) in records.iter().enumerate() {
                        match record.to_variables().get(col_name) {
                            Some(Value::Null) => {
                                data.push(Value::Null);
                                null_bm.unset(row_idx);
                            }
                            Some(Value::Missing) | None => {
                                data.push(Value::Missing);
                                missing_bm.unset(row_idx);
                            }
                            Some(v) => {
                                data.push(v.clone());
                            }
                        }
                    }
                    TypedColumn::Mixed {
                        data,
                        null: null_bm,
                        missing: missing_bm,
                    }
                }
            };

            columns.push(column);
        }

        Ok(Some(ColumnBatch {
            columns,
            names: self.schema.names.clone(),
            selection: SelectionVector::All,
            len: num_rows,
        }))
    }

    fn schema(&self) -> &BatchSchema {
        &self.schema
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

    #[test]
    fn test_row_to_batch_adapter() {
        use crate::execution::stream::InMemoryStream;
        use std::collections::VecDeque;
        use crate::common::types::Value;

        let records = vec![
            Record::new_with_variables({
                let mut v = LinkedHashMap::new();
                v.insert("x".to_string(), Value::Int(1));
                v.insert("y".to_string(), Value::String("hello".to_string().into()));
                v
            }),
            Record::new_with_variables({
                let mut v = LinkedHashMap::new();
                v.insert("x".to_string(), Value::Int(2));
                v.insert("y".to_string(), Value::String("world".to_string().into()));
                v
            }),
        ];
        let source = InMemoryStream::new(VecDeque::from(records));
        let schema = BatchSchema {
            names: vec!["x".to_string(), "y".to_string()],
            types: vec![ColumnType::Int32, ColumnType::Utf8],
        };
        let mut adapter = RowToBatchAdapter::new(Box::new(source), schema);
        let batch = adapter.next_batch().unwrap().unwrap();
        assert_eq!(batch.len, 2);
        assert_eq!(batch.columns.len(), 2);
        match &batch.columns[0] {
            TypedColumn::Int32 { data, .. } => {
                assert_eq!(data[0], 1);
                assert_eq!(data[1], 2);
            }
            _ => panic!("expected Int32"),
        }
        assert!(adapter.next_batch().unwrap().is_none());
    }

    #[test]
    fn test_adapter_round_trip() {
        use crate::execution::stream::{RecordStream, InMemoryStream};
        use std::collections::VecDeque;
        use crate::common::types::Value;

        let records: Vec<Record> = (0..3).map(|i| {
            let mut v = LinkedHashMap::new();
            v.insert("id".to_string(), Value::Int(i));
            v.insert("name".to_string(), Value::String(format!("item_{}", i).into()));
            Record::new_with_variables(v)
        }).collect();

        let source = InMemoryStream::new(VecDeque::from(records));
        let schema = BatchSchema {
            names: vec!["id".to_string(), "name".to_string()],
            types: vec![ColumnType::Int32, ColumnType::Utf8],
        };
        let batch_stream = RowToBatchAdapter::new(Box::new(source), schema);
        let mut row_stream = BatchToRowAdapter::new(Box::new(batch_stream));

        for i in 0..3 {
            let record = row_stream.next().unwrap().unwrap();
            assert_eq!(record.to_variables()["id"], Value::Int(i));
            assert_eq!(record.to_variables()["name"], Value::String(format!("item_{}", i).into()));
        }
        assert!(row_stream.next().unwrap().is_none());
    }
}
