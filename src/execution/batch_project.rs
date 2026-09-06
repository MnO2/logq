// src/execution/batch_project.rs

use crate::common::types::{Value, Variables};
use crate::execution::batch::*;
use crate::execution::batch_expression::BoundExpression;
use crate::execution::memory::{MemoryReservation, MemoryTracker, estimate_batch};
use crate::execution::types::StreamResult;
use crate::execution::types::{Expression, Named};
use crate::functions::FunctionRegistry;
use std::sync::Arc;

/// General projections retain batches while executing bound scalar kernels.
/// The existing move-only column projection remains the path for simple maps.
pub(crate) struct BatchExpressionOperator {
    child: Box<dyn BatchStream>,
    expressions: Vec<Expression>,
    bound: Vec<BoundExpression>,
    output_positions: Vec<usize>,
    input_schema: BatchSchema,
    schema: BatchSchema,
    scope: Variables,
    registry: Arc<FunctionRegistry>,
    output_memory: MemoryReservation,
}

impl BatchExpressionOperator {
    pub(crate) fn supports(named: &[Named]) -> bool {
        named.iter().all(|named| {
            matches!(named,
                Named::Expression(expression, _) if BoundExpression::supports(expression)
            )
        })
    }

    pub(crate) fn new(
        child: Box<dyn BatchStream>,
        named: &[Named],
        scope: Variables,
        registry: Arc<FunctionRegistry>,
    ) -> Self {
        let mut names = Vec::new();
        let mut original_names = Vec::with_capacity(named.len());
        let mut expressions = Vec::with_capacity(named.len());
        for (position, named) in named.iter().enumerate() {
            let Named::Expression(expression, alias) = named else {
                unreachable!("supported expression map");
            };
            let output = alias.clone().unwrap_or_else(|| format!("_{position}"));
            if let Some(position) = names.iter().position(|name| name == &output) {
                names.remove(position);
            }
            names.push(output.clone());
            original_names.push(output);
            expressions.push(expression.clone());
        }
        let output_positions = original_names
            .iter()
            .map(|name| names.iter().position(|output| name == output).unwrap())
            .collect();
        let input_schema = child.schema().clone();
        let bound = expressions
            .iter()
            .map(|expression| BoundExpression::bind(expression, &input_schema, &scope))
            .collect();
        Self {
            child,
            expressions,
            bound,
            output_positions,
            input_schema,
            schema: BatchSchema {
                types: vec![ColumnType::Mixed; names.len()],
                names,
            },
            scope,
            registry,
            output_memory: MemoryReservation::default(),
        }
    }

    pub(crate) fn with_memory_tracker(mut self, memory: MemoryTracker) -> Self {
        self.output_memory = MemoryReservation::new(memory);
        self
    }
}

impl BatchStream for BatchExpressionOperator {
    fn next_batch(&mut self) -> StreamResult<Option<ColumnBatch>> {
        self.output_memory.resize(0)?;
        let Some(batch) = self.child.next_batch()? else {
            return Ok(None);
        };
        if batch.names != self.input_schema.names {
            self.input_schema.names = batch.names.clone();
            self.bound = self
                .expressions
                .iter()
                .map(|expression| BoundExpression::bind(expression, &self.input_schema, &self.scope))
                .collect();
        }
        let mut passthrough = vec![None; self.schema.names.len()];
        // Moving an arena can retain inactive rows or spare capacity that the
        // old materialization discarded. Preserve budgeted-query behavior until
        // ownership transfer and compaction have shared memory accounting.
        if !self.output_memory.is_enabled() {
            let mut final_expressions = vec![None; self.schema.names.len()];
            for (expression, output) in self.bound.iter().zip(&self.output_positions) {
                final_expressions[*output] = expression.direct_column();
            }
            let mut used = vec![false; batch.columns.len()];
            for (output, source) in final_expressions.into_iter().enumerate().rev() {
                if let Some(source) = source {
                    if !used[source] {
                        passthrough[output] = Some(source);
                        used[source] = true;
                    }
                }
            }
        }
        let mut columns: Vec<Option<Vec<Value>>> = passthrough
            .iter()
            .map(|source| source.is_none().then(|| vec![Value::Missing; batch.len]))
            .collect();
        for row in (0..batch.len).filter(|&row| batch.selection.is_active(row, batch.len)) {
            // Keep row/SELECT-list evaluation order, including overwritten
            // aliases whose expressions can still report an error.
            for (expression, output) in self.bound.iter_mut().zip(&self.output_positions) {
                if columns[*output].is_none() && expression.direct_column().is_some() {
                    continue;
                }
                let value = expression.evaluate(&batch, row, &self.registry)?;
                if let Some(column) = &mut columns[*output] {
                    column[row] = value;
                }
            }
        }
        let mut input: Vec<_> = batch.columns.into_iter().map(Some).collect();
        let output = ColumnBatch {
            columns: columns
                .into_iter()
                .zip(passthrough)
                .map(|(values, source)| match source {
                    Some(source) => input[source].take().expect("each passthrough moves its source once"),
                    None => crate::execution::json_batch_scan::typed_column(values.expect("computed output storage")),
                })
                .collect(),
            names: self.schema.names.clone(),
            selection: batch.selection,
            len: batch.len,
        };
        if self.output_memory.is_enabled() {
            self.output_memory.resize(estimate_batch(&output))?;
        }
        Ok(Some(output))
    }

    fn schema(&self) -> &BatchSchema {
        &self.schema
    }
    fn close(&self) {
        self.child.close();
    }
}

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
    use crate::syntax::ast::{CastType, PathExpr, PathSegment};

    struct ExpressionBatch {
        batch: Option<ColumnBatch>,
        schema: BatchSchema,
    }

    impl BatchStream for ExpressionBatch {
        fn next_batch(&mut self) -> StreamResult<Option<ColumnBatch>> {
            Ok(self.batch.take())
        }
        fn schema(&self) -> &BatchSchema {
            &self.schema
        }
        fn close(&self) {}
    }

    fn expression_operator(
        batch: ColumnBatch,
        expressions: Vec<Named>,
        registry: Arc<FunctionRegistry>,
    ) -> BatchExpressionOperator {
        let schema = BatchSchema {
            names: batch.names.clone(),
            types: vec![ColumnType::Mixed; batch.names.len()],
        };
        BatchExpressionOperator::new(
            Box::new(ExpressionBatch {
                batch: Some(batch),
                schema,
            }),
            &expressions,
            Variables::new(),
            registry,
        )
    }

    fn field(name: &str) -> Expression {
        Expression::Variable(PathExpr::new(vec![PathSegment::AttrName(name.into())]))
    }

    #[test]
    fn mixed_projection_moves_passthrough_after_computed_readers() {
        let payload = "large payload ".repeat(1024);
        let column = crate::execution::json_batch_scan::typed_column(vec![
            Value::String(payload.clone().into()),
            Value::Null,
            Value::Missing,
        ]);
        let pointer = match &column {
            TypedColumn::Utf8 { data, .. } => data.as_ptr(),
            _ => panic!("utf8"),
        };
        let mut selected = Bitmap::all_unset(3);
        selected.set(0);
        selected.set(2);
        let batch = ColumnBatch {
            columns: vec![column],
            names: vec!["payload".into()],
            selection: SelectionVector::Bitmap(selected),
            len: 3,
        };
        let mut operator = expression_operator(
            batch,
            vec![
                Named::Expression(field("payload"), Some("original".into())),
                Named::Expression(
                    Expression::Cast(Box::new(field("payload")), CastType::Varchar),
                    Some("computed".into()),
                ),
                Named::Expression(field("payload"), Some("again".into())),
            ],
            Arc::new(crate::functions::register_all().unwrap()),
        );
        let output = operator.next_batch().unwrap().unwrap();
        assert_eq!(output.selection.count_active(output.len), 2);
        for column in &output.columns {
            assert_eq!(
                BatchToRowAdapter::extract_value(column, 0),
                Value::String(payload.clone().into())
            );
            assert_eq!(BatchToRowAdapter::extract_value(column, 2), Value::Missing);
        }
        assert!(
            output
                .columns
                .iter()
                .any(|column| matches!(column, TypedColumn::Utf8 { data, .. } if data.as_ptr() == pointer)),
            "one passthrough must retain the original string arena"
        );
    }

    #[test]
    fn mixed_projection_preserves_overwritten_effects_and_row_order() {
        use crate::functions::{Arity, FunctionDef, NullHandling};
        use std::sync::Mutex;
        let calls = Arc::new(Mutex::new(Vec::new()));
        let recorded = calls.clone();
        let mut registry = FunctionRegistry::new();
        registry
            .register(FunctionDef {
                name: "record".into(),
                arity: Arity::Exact(1),
                null_handling: NullHandling::Custom,
                func: Box::new(move |values| {
                    recorded.lock().unwrap().push(values[0].clone());
                    Ok(values[0].clone())
                }),
            })
            .unwrap();
        let expression = Expression::Function("record".into(), vec![Named::Expression(field("x"), None)]);
        let batch = ColumnBatch {
            columns: vec![crate::execution::json_batch_scan::typed_column(vec![
                Value::Int(1),
                Value::Int(2),
            ])],
            names: vec!["x".into()],
            selection: SelectionVector::All,
            len: 2,
        };
        let mut operator = expression_operator(
            batch,
            vec![
                Named::Expression(expression.clone(), Some("alias".into())),
                Named::Expression(field("x"), Some("alias".into())),
                Named::Expression(expression, Some("other".into())),
            ],
            Arc::new(registry),
        );
        let output = operator.next_batch().unwrap().unwrap();
        assert_eq!(output.names, ["alias", "other"]);
        assert_eq!(
            *calls.lock().unwrap(),
            vec![Value::Int(1), Value::Int(1), Value::Int(2), Value::Int(2)]
        );
        assert_eq!(BatchToRowAdapter::extract_value(&output.columns[0], 1), Value::Int(2));
    }

    #[test]
    fn mixed_projection_does_not_skip_overwritten_errors() {
        let batch = ColumnBatch {
            columns: vec![crate::execution::json_batch_scan::typed_column(vec![Value::String(
                "bad".into(),
            )])],
            names: vec!["x".into()],
            selection: SelectionVector::All,
            len: 1,
        };
        let mut operator = expression_operator(
            batch,
            vec![
                Named::Expression(
                    Expression::Cast(Box::new(field("x")), CastType::Int),
                    Some("alias".into()),
                ),
                Named::Expression(field("x"), Some("alias".into())),
            ],
            Arc::new(crate::functions::register_all().unwrap()),
        );
        assert!(operator.next_batch().is_err());
    }

    #[test]
    fn mixed_projection_budget_drops_inactive_payload() {
        let batch = ColumnBatch {
            columns: vec![crate::execution::json_batch_scan::typed_column(vec![
                Value::String("ok".into()),
                Value::String("unused".repeat(128 * 1024).into()),
            ])],
            names: vec!["payload".into()],
            selection: SelectionVector::Bitmap({
                let mut selected = Bitmap::all_unset(2);
                selected.set(0);
                selected
            }),
            len: 2,
        };
        let mut operator = expression_operator(
            batch,
            vec![
                Named::Expression(field("payload"), Some("payload".into())),
                Named::Expression(Expression::Constant(Value::Int(1)), Some("computed".into())),
            ],
            Arc::new(crate::functions::register_all().unwrap()),
        )
        .with_memory_tracker(MemoryTracker::new(Some(4096)));
        let output = operator.next_batch().unwrap().unwrap();
        assert_eq!(
            BatchToRowAdapter::extract_value(&output.columns[0], 0),
            Value::String("ok".into())
        );
        assert_eq!(BatchToRowAdapter::extract_value(&output.columns[0], 1), Value::Missing);
    }

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
