// src/execution/batch_project.rs

use crate::execution::batch::*;
use crate::execution::types::StreamResult;

/// Projects (selects) a subset of columns from a ColumnBatch.
pub(crate) struct BatchProjectOperator {
    child: Box<dyn BatchStream>,
    output_columns: Vec<String>,
    schema: BatchSchema,
}

impl BatchProjectOperator {
    pub fn new(child: Box<dyn BatchStream>, output_columns: Vec<String>) -> Self {
        let child_schema = child.schema();
        let types: Vec<ColumnType> = output_columns.iter()
            .filter_map(|name| {
                child_schema.names.iter().position(|n| n == name)
                    .map(|i| child_schema.types[i].clone())
            })
            .collect();
        let schema = BatchSchema {
            names: output_columns.clone(),
            types,
        };
        Self { child, output_columns, schema }
    }
}

impl BatchStream for BatchProjectOperator {
    fn next_batch(&mut self) -> StreamResult<Option<ColumnBatch>> {
        match self.child.next_batch()? {
            Some(batch) => {
                let ColumnBatch { columns, names, selection, len } = batch;
                let mut col_map: Vec<(String, TypedColumn)> = names.into_iter()
                    .zip(columns.into_iter()).collect();

                let mut new_columns = Vec::with_capacity(self.output_columns.len());
                let mut new_names = Vec::with_capacity(self.output_columns.len());

                for output_name in &self.output_columns {
                    if let Some(pos) = col_map.iter().position(|(n, _)| n == output_name) {
                        let (name, col) = col_map.remove(pos);
                        new_columns.push(col);
                        new_names.push(name);
                    }
                }

                Ok(Some(ColumnBatch {
                    columns: new_columns,
                    names: new_names,
                    selection,
                    len,
                }))
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
    fn test_project_selects_columns() {
        let col_a = TypedColumn::Int32 {
            data: PaddedVec::from_vec(vec![1, 2, 3]),
            null: Bitmap::all_set(3),
            missing: Bitmap::all_set(3),
        };
        let col_b = TypedColumn::Int32 {
            data: PaddedVec::from_vec(vec![10, 20, 30]),
            null: Bitmap::all_set(3),
            missing: Bitmap::all_set(3),
        };
        let batch = ColumnBatch {
            columns: vec![col_a, col_b],
            names: vec!["a".to_string(), "b".to_string()],
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

        let schema = BatchSchema {
            names: vec!["a".to_string(), "b".to_string()],
            types: vec![ColumnType::Int32, ColumnType::Int32],
        };
        let mut proj = BatchProjectOperator::new(
            Box::new(OneBatch { batch: Some(batch), schema }),
            vec!["b".to_string()],
        );
        let result = proj.next_batch().unwrap().unwrap();
        assert_eq!(result.columns.len(), 1);
        assert_eq!(result.names, vec!["b".to_string()]);
        match &result.columns[0] {
            TypedColumn::Int32 { data, .. } => {
                assert_eq!(data[0], 10);
                assert_eq!(data[1], 20);
                assert_eq!(data[2], 30);
            }
            _ => panic!("expected Int32"),
        }
    }

    #[test]
    fn test_project_preserves_selection() {
        let col = TypedColumn::Int32 {
            data: PaddedVec::from_vec(vec![1, 2, 3]),
            null: Bitmap::all_set(3),
            missing: Bitmap::all_set(3),
        };
        let mut sel = Bitmap::all_unset(3);
        sel.set(0); sel.set(2);
        let batch = ColumnBatch {
            columns: vec![col],
            names: vec!["x".to_string()],
            selection: SelectionVector::Bitmap(sel),
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
        let mut proj = BatchProjectOperator::new(
            Box::new(OneBatch { batch: Some(batch), schema }),
            vec!["x".to_string()],
        );
        let result = proj.next_batch().unwrap().unwrap();
        assert_eq!(result.selection.count_active(result.len), 2);
    }
}
