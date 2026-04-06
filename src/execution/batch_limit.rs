// src/execution/batch_limit.rs

use crate::execution::batch::*;
use crate::execution::types::StreamResult;
use crate::simd::selection::SelectionVector;

pub(crate) struct BatchLimitOperator {
    child: Box<dyn BatchStream>,
    remaining: usize,
    schema: BatchSchema,
}

impl BatchLimitOperator {
    pub fn new(child: Box<dyn BatchStream>, limit: u32) -> Self {
        let schema = BatchSchema {
            names: child.schema().names.clone(),
            types: child.schema().types.clone(),
        };
        Self { child, remaining: limit as usize, schema }
    }
}

impl BatchStream for BatchLimitOperator {
    fn next_batch(&mut self) -> StreamResult<Option<ColumnBatch>> {
        if self.remaining == 0 {
            return Ok(None);
        }

        match self.child.next_batch()? {
            Some(mut batch) => {
                let active = batch.selection.count_active(batch.len);

                if active <= self.remaining {
                    self.remaining -= active;
                    Ok(Some(batch))
                } else {
                    // Truncate: keep only `self.remaining` active rows
                    let mut keep = self.remaining;
                    let mut bm = batch.selection.to_bitmap(batch.len);
                    for row in 0..batch.len {
                        if bm.is_set(row) {
                            if keep == 0 {
                                bm.unset(row);
                            } else {
                                keep -= 1;
                            }
                        }
                    }
                    batch.selection = SelectionVector::Bitmap(bm);
                    self.remaining = 0;
                    Ok(Some(batch))
                }
            }
            None => Ok(None),
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
    use crate::simd::bitmap::Bitmap;
    use crate::simd::padded_vec::PaddedVec;
    use crate::simd::selection::SelectionVector;

    #[test]
    fn test_limit_truncates() {
        let batches: Vec<ColumnBatch> = (0..3).map(|i| {
            ColumnBatch {
                columns: vec![TypedColumn::Int32 {
                    data: PaddedVec::from_vec(vec![i; 10]),
                    null: Bitmap::all_set(10),
                    missing: Bitmap::all_set(10),
                }],
                names: vec!["x".to_string()],
                selection: SelectionVector::All,
                len: 10,
            }
        }).collect();

        struct MultiBatch { batches: Vec<ColumnBatch>, schema: BatchSchema }
        impl BatchStream for MultiBatch {
            fn next_batch(&mut self) -> crate::execution::types::StreamResult<Option<ColumnBatch>> {
                if !self.batches.is_empty() {
                    Ok(Some(self.batches.remove(0)))
                } else {
                    Ok(None)
                }
            }
            fn schema(&self) -> &BatchSchema { &self.schema }
            fn close(&self) {}
        }

        let schema = BatchSchema { names: vec!["x".to_string()], types: vec![ColumnType::Int32] };
        let mut limit = BatchLimitOperator::new(
            Box::new(MultiBatch { batches, schema }), 15,
        );

        // First batch: 10 rows, all pass (15 - 10 = 5 remaining)
        let b1 = limit.next_batch().unwrap().unwrap();
        assert_eq!(b1.selection.count_active(b1.len), 10);

        // Second batch: only 5 of 10 rows should be active
        let b2 = limit.next_batch().unwrap().unwrap();
        assert_eq!(b2.selection.count_active(b2.len), 5);

        // Third batch: should be None (remaining=0)
        assert!(limit.next_batch().unwrap().is_none());
    }

    #[test]
    fn test_limit_exact_fit() {
        let batch = ColumnBatch {
            columns: vec![TypedColumn::Int32 {
                data: PaddedVec::from_vec(vec![1, 2, 3]),
                null: Bitmap::all_set(3),
                missing: Bitmap::all_set(3),
            }],
            names: vec!["x".to_string()],
            selection: SelectionVector::All,
            len: 3,
        };

        struct OneBatch { batch: Option<ColumnBatch>, schema: BatchSchema }
        impl BatchStream for OneBatch {
            fn next_batch(&mut self) -> crate::execution::types::StreamResult<Option<ColumnBatch>> {
                Ok(self.batch.take())
            }
            fn schema(&self) -> &BatchSchema { &self.schema }
            fn close(&self) {}
        }

        let schema = BatchSchema { names: vec!["x".to_string()], types: vec![ColumnType::Int32] };
        let mut limit = BatchLimitOperator::new(
            Box::new(OneBatch { batch: Some(batch), schema }), 3,
        );

        let b = limit.next_batch().unwrap().unwrap();
        assert_eq!(b.selection.count_active(b.len), 3);
        assert!(limit.next_batch().unwrap().is_none());
    }

    #[test]
    fn test_limit_with_selection() {
        let mut sel = Bitmap::all_unset(6);
        sel.set(0); sel.set(2); sel.set(4); // 3 active rows
        let batch = ColumnBatch {
            columns: vec![TypedColumn::Int32 {
                data: PaddedVec::from_vec(vec![10, 20, 30, 40, 50, 60]),
                null: Bitmap::all_set(6),
                missing: Bitmap::all_set(6),
            }],
            names: vec!["x".to_string()],
            selection: SelectionVector::Bitmap(sel),
            len: 6,
        };

        struct OneBatch { batch: Option<ColumnBatch>, schema: BatchSchema }
        impl BatchStream for OneBatch {
            fn next_batch(&mut self) -> crate::execution::types::StreamResult<Option<ColumnBatch>> {
                Ok(self.batch.take())
            }
            fn schema(&self) -> &BatchSchema { &self.schema }
            fn close(&self) {}
        }

        let schema = BatchSchema { names: vec!["x".to_string()], types: vec![ColumnType::Int32] };
        let mut limit = BatchLimitOperator::new(
            Box::new(OneBatch { batch: Some(batch), schema }), 2,
        );

        let b = limit.next_batch().unwrap().unwrap();
        assert_eq!(b.selection.count_active(b.len), 2);
        assert!(b.selection.is_active(0, b.len));
        assert!(b.selection.is_active(2, b.len));
        assert!(!b.selection.is_active(4, b.len));
    }
}
