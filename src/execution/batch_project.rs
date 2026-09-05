// src/execution/batch_project.rs

use crate::execution::batch::*;
use crate::execution::types::StreamResult;

/// Projects (selects) a subset of columns from a ColumnBatch.
pub(crate) struct BatchProjectOperator {
    child: Box<dyn BatchStream>,
    projection: Vec<(String, String)>,
    schema: BatchSchema,
    scope: crate::common::types::Variables,
}

impl BatchProjectOperator {
    #[cfg(test)]
    pub fn new(child: Box<dyn BatchStream>, output_columns: Vec<String>) -> Self {
        Self::with_projection(
            child,
            output_columns.into_iter().map(|name| (name.clone(), name)).collect(),
        )
    }

    pub fn with_projection(child: Box<dyn BatchStream>, projection: Vec<(String, String)>) -> Self {
        // LinkedHashMap moves overwritten output names to their last position.
        let mut unique: Vec<(String, String)> = Vec::new();
        for (source, output) in projection {
            if let Some(index) = unique.iter().position(|(_, name)| name == &output) {
                unique.remove(index);
            }
            unique.push((source, output));
        }
        let schema = BatchSchema {
            names: unique.iter().map(|(_, output)| output.clone()).collect(),
            types: unique
                .iter()
                .map(|(source, _)| {
                    child
                        .schema()
                        .names
                        .iter()
                        .position(|n| n == source)
                        .map(|i| child.schema().types[i].clone())
                        .unwrap_or(ColumnType::Mixed)
                })
                .collect(),
        };
        Self {
            child,
            projection: unique,
            schema,
            scope: crate::common::types::Variables::new(),
        }
    }

    pub(crate) fn with_scope(mut self, scope: crate::common::types::Variables) -> Self {
        self.scope = scope;
        self
    }
}

impl BatchStream for BatchProjectOperator {
    fn next_batch(&mut self) -> StreamResult<Option<ColumnBatch>> {
        match self.child.next_batch()? {
            Some(batch) => {
                let ColumnBatch {
                    columns,
                    names,
                    selection,
                    len,
                } = batch;
                let mut columns: Vec<Option<TypedColumn>> = columns.into_iter().map(Some).collect();
                let mut new_columns = Vec::with_capacity(self.projection.len());
                let mut new_names = Vec::with_capacity(self.projection.len());
                for (index, (source, output)) in self.projection.iter().enumerate() {
                    let column = if let Some(pos) = names.iter().position(|name| name == source) {
                        if self.projection[index + 1..].iter().any(|(name, _)| name == source) {
                            let data: Vec<_> = (0..len)
                                .map(|row| {
                                    BatchToRowAdapter::extract_value(
                                        columns[pos].as_ref().expect("column still needed"),
                                        row,
                                    )
                                })
                                .collect();
                            let mut null = crate::simd::bitmap::Bitmap::all_set(len);
                            let mut missing = crate::simd::bitmap::Bitmap::all_set(len);
                            for (row, value) in data.iter().enumerate() {
                                match value {
                                    crate::common::types::Value::Null => null.unset(row),
                                    crate::common::types::Value::Missing => missing.unset(row),
                                    _ => {}
                                }
                            }
                            TypedColumn::Mixed { data, null, missing }
                        } else {
                            columns[pos].take().expect("last column use")
                        }
                    } else {
                        let value = self
                            .scope
                            .get(source)
                            .cloned()
                            .unwrap_or(crate::common::types::Value::Missing);
                        TypedColumn::Mixed {
                            null: if matches!(value, crate::common::types::Value::Null) {
                                crate::simd::bitmap::Bitmap::all_unset(len)
                            } else {
                                crate::simd::bitmap::Bitmap::all_set(len)
                            },
                            missing: if matches!(value, crate::common::types::Value::Missing) {
                                crate::simd::bitmap::Bitmap::all_unset(len)
                            } else {
                                crate::simd::bitmap::Bitmap::all_set(len)
                            },
                            data: vec![value; len],
                        }
                    };
                    new_columns.push(column);
                    new_names.push(output.clone());
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

        struct OneBatch {
            batch: Option<ColumnBatch>,
            schema: BatchSchema,
        }
        impl BatchStream for OneBatch {
            fn next_batch(&mut self) -> crate::execution::types::StreamResult<Option<ColumnBatch>> {
                Ok(self.batch.take())
            }
            fn schema(&self) -> &BatchSchema {
                &self.schema
            }
            fn close(&self) {}
        }

        let schema = BatchSchema {
            names: vec!["a".to_string(), "b".to_string()],
            types: vec![ColumnType::Int32, ColumnType::Int32],
        };
        let mut proj = BatchProjectOperator::new(
            Box::new(OneBatch {
                batch: Some(batch),
                schema,
            }),
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
        sel.set(0);
        sel.set(2);
        let batch = ColumnBatch {
            columns: vec![col],
            names: vec!["x".to_string()],
            selection: SelectionVector::Bitmap(sel),
            len: 3,
        };

        struct OneBatch {
            batch: Option<ColumnBatch>,
            schema: BatchSchema,
        }
        impl BatchStream for OneBatch {
            fn next_batch(&mut self) -> crate::execution::types::StreamResult<Option<ColumnBatch>> {
                Ok(self.batch.take())
            }
            fn schema(&self) -> &BatchSchema {
                &self.schema
            }
            fn close(&self) {}
        }

        let schema = BatchSchema {
            names: vec!["x".to_string()],
            types: vec![ColumnType::Int32],
        };
        let mut proj = BatchProjectOperator::new(
            Box::new(OneBatch {
                batch: Some(batch),
                schema,
            }),
            vec!["x".to_string()],
        );
        let result = proj.next_batch().unwrap().unwrap();
        assert_eq!(result.selection.count_active(result.len), 2);
    }
}
