// src/execution/batch_filter.rs

use crate::execution::batch::*;
use crate::execution::batch_predicate::evaluate_batch_predicate;
use crate::execution::types::{Formula, StreamResult};
use crate::simd::selection::SelectionVector;
use crate::common::types::Variables;
use crate::functions::FunctionRegistry;
use std::sync::Arc;

pub(crate) struct BatchFilterOperator {
    child: Box<dyn BatchStream>,
    formula: Formula,
    variables: Variables,
    registry: Arc<FunctionRegistry>,
    schema: BatchSchema,
}

impl BatchFilterOperator {
    pub fn new(
        child: Box<dyn BatchStream>,
        formula: Formula,
        variables: Variables,
        registry: Arc<FunctionRegistry>,
    ) -> Self {
        let schema = BatchSchema {
            names: child.schema().names.clone(),
            types: child.schema().types.clone(),
        };
        // Fold constant sub-expressions before execution
        let formula = formula.fold_constants();
        Self { child, formula, variables, registry, schema }
    }
}

impl BatchStream for BatchFilterOperator {
    fn next_batch(&mut self) -> StreamResult<Option<ColumnBatch>> {
        while let Some(mut batch) = self.child.next_batch()? {
            let result_bm = evaluate_batch_predicate(
                &self.formula, &batch, &self.variables, &self.registry,
            )?;

            // Combine with existing selection
            let new_selection = match batch.selection {
                SelectionVector::All => SelectionVector::Bitmap(result_bm),
                SelectionVector::Bitmap(ref existing) => {
                    SelectionVector::Bitmap(existing.and(&result_bm))
                }
            };

            batch.selection = new_selection;

            if batch.selection.any_active(batch.len) {
                return Ok(Some(batch));
            }
            // All rows filtered out — skip batch, pull next
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
    use crate::simd::bitmap::Bitmap;
    use crate::simd::padded_vec::PaddedVecBuilder;
    use crate::execution::types::{Formula, Expression, Relation};
    use crate::syntax::ast::{PathExpr, PathSegment};
    use crate::common::types::Value;

    #[test]
    fn test_batch_filter_narrows_selection() {
        let mut data_builder = PaddedVecBuilder::<u8>::new();
        let mut offsets_builder = PaddedVecBuilder::<u32>::with_capacity(5);
        offsets_builder.push(0);
        for s in &["200", "404", "200", "500"] {
            data_builder.extend_from_slice(s.as_bytes());
            offsets_builder.push(data_builder.len() as u32);
        }
        let col = TypedColumn::Utf8 {
            data: data_builder.seal(),
            offsets: offsets_builder.seal(),
            null: Bitmap::all_set(4),
            missing: Bitmap::all_set(4),
        };
        let batch = ColumnBatch {
            columns: vec![col],
            names: vec!["status".to_string()],
            selection: SelectionVector::All,
            len: 4,
        };

        struct OneBatch { batch: Option<ColumnBatch>, schema: BatchSchema }
        impl BatchStream for OneBatch {
            fn next_batch(&mut self) -> crate::execution::types::StreamResult<Option<ColumnBatch>> {
                Ok(self.batch.take())
            }
            fn schema(&self) -> &BatchSchema { &self.schema }
            fn close(&self) {}
        }

        let formula = Formula::Predicate(
            Relation::Equal,
            Box::new(Expression::Variable(PathExpr::new(vec![
                PathSegment::AttrName("status".to_string()),
            ]))),
            Box::new(Expression::Constant(Value::String("200".to_string().into()))),
        );

        let registry = Arc::new(crate::functions::register_all().unwrap());
        let schema = BatchSchema { names: vec!["status".to_string()], types: vec![ColumnType::Utf8] };
        let mut filter = BatchFilterOperator::new(
            Box::new(OneBatch { batch: Some(batch), schema }),
            formula,
            Variables::new(),
            registry,
        );

        let result = filter.next_batch().unwrap().unwrap();
        assert_eq!(result.selection.count_active(result.len), 2);
        assert!(result.selection.is_active(0, result.len));
        assert!(!result.selection.is_active(1, result.len));
        assert!(result.selection.is_active(2, result.len));
        assert!(!result.selection.is_active(3, result.len));
    }

    #[test]
    fn test_batch_filter_skips_empty_batches() {
        use crate::simd::padded_vec::PaddedVec;
        // All rows filtered out
        let col = TypedColumn::Int32 {
            data: PaddedVec::from_vec(vec![1, 2, 3]),
            null: Bitmap::all_set(3),
            missing: Bitmap::all_set(3),
        };
        let batch = ColumnBatch {
            columns: vec![col],
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

        // Filter: x > 100 — nothing matches
        let formula = Formula::Predicate(
            Relation::MoreThan,
            Box::new(Expression::Variable(PathExpr::new(vec![
                PathSegment::AttrName("x".to_string()),
            ]))),
            Box::new(Expression::Constant(Value::Int(100))),
        );

        let registry = Arc::new(crate::functions::register_all().unwrap());
        let schema = BatchSchema { names: vec!["x".to_string()], types: vec![ColumnType::Int32] };
        let mut filter = BatchFilterOperator::new(
            Box::new(OneBatch { batch: Some(batch), schema }),
            formula,
            Variables::new(),
            registry,
        );

        // Should return None since all rows are filtered
        assert!(filter.next_batch().unwrap().is_none());
    }
}
