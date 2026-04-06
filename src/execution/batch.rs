// src/execution/batch.rs

use crate::common::types::Value;
use crate::execution::types::StreamResult;
use crate::simd::bitmap::Bitmap;
use crate::simd::padded_vec::PaddedVec;
use crate::simd::selection::SelectionVector;

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
}
