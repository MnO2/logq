// src/execution/batch_orderby.rs

use crate::common::types::Value;
use crate::execution::batch::*;
use crate::execution::types::{Ordering, StreamResult};
use crate::simd::bitmap::Bitmap;
use crate::simd::padded_vec::PaddedVec;
use crate::simd::selection::SelectionVector;
use crate::syntax::ast::{PathExpr, PathSegment};
use ordered_float::OrderedFloat;
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
}

impl BatchOrderByOperator {
    pub fn new(
        child: Box<dyn BatchStream>,
        sort_columns: Vec<PathExpr>,
        orderings: Vec<Ordering>,
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
        }
    }

    fn consume_and_sort(&mut self) -> StreamResult<()> {
        // Phase 1: Collect all active rows into accumulated columns
        let mut all_values: Vec<Vec<Value>> = Vec::new();
        let num_cols = self.schema.names.len();
        for _ in 0..num_cols {
            all_values.push(Vec::new());
        }

        while let Some(batch) = self.child.next_batch()? {
            for row in 0..batch.len {
                if !batch.selection.is_active(row, batch.len) {
                    continue;
                }
                for (col_idx, col) in batch.columns.iter().enumerate() {
                    let val = BatchToRowAdapter::extract_value(col, row);
                    all_values[col_idx].push(val);
                }
            }
        }

        let total_rows = if num_cols > 0 { all_values[0].len() } else { 0 };
        if total_rows == 0 {
            return Ok(());
        }

        // Phase 2: Resolve sort column indices
        let sort_col_indices: Vec<Option<usize>> = self.sort_columns.iter().map(|path| {
            if let Some(PathSegment::AttrName(name)) = path.path_segments.last() {
                self.schema.names.iter().position(|n| n == name)
            } else {
                None
            }
        }).collect();

        // Phase 3: Build permutation index and sort
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
        for col_idx in 0..num_cols {
            let mut col_data = Vec::with_capacity(total_rows);
            for &idx in &indices {
                col_data.push(all_values[col_idx][idx].clone());
            }
            sorted_values.push(col_data);
        }

        // Emit in BATCH_SIZE chunks
        let mut offset = 0;
        while offset < total_rows {
            let chunk_len = (total_rows - offset).min(BATCH_SIZE);
            let mut columns = Vec::with_capacity(num_cols);
            for col_idx in 0..num_cols {
                let chunk: Vec<Value> = sorted_values[col_idx][offset..offset + chunk_len].to_vec();
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

        Ok(())
    }
}

/// Compare two Values for ordering.
fn compare_values(a: &Value, b: &Value) -> cmp::Ordering {
    match (a, b) {
        (Value::Int(x), Value::Int(y)) => x.cmp(y),
        (Value::Float(x), Value::Float(y)) => x.cmp(y),
        (Value::Int(x), Value::Float(y)) => OrderedFloat(*x as f32).cmp(y),
        (Value::Float(x), Value::Int(y)) => x.cmp(&OrderedFloat(*y as f32)),
        (Value::String(x), Value::String(y)) => x.cmp(y),
        (Value::DateTime(x), Value::DateTime(y)) => x.cmp(y),
        (Value::Boolean(x), Value::Boolean(y)) => x.cmp(y),
        (Value::Null, Value::Null) => cmp::Ordering::Equal,
        (Value::Missing, Value::Missing) => cmp::Ordering::Equal,
        (Value::Null | Value::Missing, _) => cmp::Ordering::Greater,
        (_, Value::Null | Value::Missing) => cmp::Ordering::Less,
        _ => cmp::Ordering::Equal,
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
            Box::new(OneBatch { batch: Some(batch), schema }),
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
            Box::new(OneBatch { batch: Some(batch), schema }),
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
            Box::new(OneBatch { batch: Some(batch), schema }),
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
        struct Empty { schema: BatchSchema }
        impl BatchStream for Empty {
            fn next_batch(&mut self) -> StreamResult<Option<ColumnBatch>> { Ok(None) }
            fn schema(&self) -> &BatchSchema { &self.schema }
            fn close(&self) {}
        }

        let sort_col = PathExpr::new(vec![PathSegment::AttrName("x".to_string())]);
        let mut op = BatchOrderByOperator::new(
            Box::new(Empty { schema }),
            vec![sort_col],
            vec![Ordering::Asc],
        );

        assert!(op.next_batch().unwrap().is_none());
    }
}
