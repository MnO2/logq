//! Bounded, benchmark-only comparison of a fixed built-in Plus expression.
use crate::common::types::{Value, Variables};
use crate::execution::batch::{BatchSchema, BatchToRowAdapter, ColumnBatch, ColumnType, TypedColumn};
use crate::execution::batch_expression::BoundExpression;
use crate::execution::json_batch_scan::typed_column;
use crate::execution::types::{Expression, Named};
use crate::functions::registry::ResolvedFunction;
use crate::simd::bitmap::Bitmap;
use crate::simd::padded_vec::{PaddedVec, PaddedVecBuilder};
use crate::simd::selection::SelectionVector;
use crate::syntax::ast::{PathExpr, PathSegment};
use ordered_float::OrderedFloat;
use serde::Serialize;
use std::hint::black_box;
use std::time::Instant;

type ProbeResult<T> = Result<T, Box<dyn std::error::Error>>;

#[derive(Clone, Debug, Serialize)]
pub struct ExpressionProbeConfig {
    pub rows: usize,
    pub chain_length: usize,
    pub nullable: bool,
    pub active_percent: u8,
    pub reverse: bool,
}

pub type ExpressionProbeReport = serde_json::Value;

pub fn profile_expressions(config: ExpressionProbeConfig) -> ProbeResult<ExpressionProbeReport> {
    if config.rows == 0
        || config.rows > 2_000_000
        || ![1, 16].contains(&config.chain_length)
        || config.active_percent > 100
    {
        return Err("require 1..=2000000 rows, chain length 1 or 16, and active percent 0..=100".into());
    }
    let batch = input(&config);
    let schema = BatchSchema {
        names: batch.names.clone(),
        types: vec![ColumnType::Float32],
    };
    let registry = crate::functions::register_all()?;
    let mut expression = Expression::Variable(PathExpr::new(vec![PathSegment::AttrName("v".into())]));
    for _ in 0..config.chain_length {
        expression = Expression::Function(
            "plus".into(),
            vec![
                Named::Expression(expression, None),
                Named::Expression(Expression::Constant(Value::Float(OrderedFloat(0.5))), None),
            ],
        );
    }
    let mut bound = BoundExpression::bind(&expression, &schema, &Variables::new(), &registry);
    let scalar = registry.resolve("plus").expect("known built-in fixture");
    let oracle = summarize(
        &config,
        (0..batch.len).map(|row| oracle_value(&batch, row, config.chain_length)),
    );
    let mut order = vec!["bound_expression", "registered_scalar", "typed_f32"];
    if config.reverse {
        order.reverse();
    }
    let mut kernels = Vec::new();
    for name in order {
        // Complete value preflight before the timed invocation. Binding, input
        // construction, validation and result disposal are outside each timer.
        let checked = run_kernel(name, &batch, &mut bound, &scalar, config.chain_length)?;
        validate_output(&batch, &checked, config.chain_length)?;
        drop(checked);
        let start = Instant::now();
        let output = run_kernel(name, black_box(&batch), &mut bound, &scalar, config.chain_length)?;
        black_box(&output);
        let elapsed_ns = start.elapsed().as_nanos().min(u64::MAX as u128) as u64;
        validate_output(&batch, &output, config.chain_length)?;
        let counts_and_sum = summarize(
            &config,
            (0..batch.len).map(|row| BatchToRowAdapter::extract_value(&output, row)),
        );
        if counts_and_sum != oracle {
            return Err("count/sum validation mismatch".into());
        }
        kernels.push(serde_json::json!({"kernel": name, "elapsed_ns": elapsed_ns, "checked_rows": batch.len, "counts_and_sum": counts_and_sum}));
    }
    Ok(serde_json::json!({
        "version": 1, "config": config, "validation": "passed", "oracle": oracle, "kernels": kernels,
        "expression": "left-associated v + Float32(0.5), repeated exactly chain_length times",
        "input": "one preparsed Float32 column; first rows include NaN, infinities, signed zero, subnormal, f32 extrema and +/-2^24; remaining v=(row%1021)*0.25-128; nullable: row%13 MISSING else row%11 NULL; active: row%100<active_percent",
        "timing_scope": "same borrowed input and pre-bound functions; kernel evaluation and all output allocation/construction included; registry/binding/input setup, full-row checks, summary, output disposal and formatting excluded; each kernel has an untimed preflight",
        "outputs": "bound and registered kernels construct Vec<Value> then production typed_column; typed kernel directly builds Float32 storage/bitmaps; all inactive rows materialize MISSING; all-nullish legacy output may use Mixed storage",
        "semantic_scope": "fixed register_all built-in Plus only, not a planner substitution for custom functions; no casts, integer arithmetic, overflow policy, CASE or reassociation; each addition rounds to f32",
        "validation_scope": "every output row checked before and after its timed run; finite and infinite float bits checked exactly, NaNs compared as NaN, NULL/MISSING/selection checked individually; count and f64 SUM additionally checked",
    }))
}

fn input(config: &ExpressionProbeConfig) -> ColumnBatch {
    let special = [
        f32::NAN,
        f32::INFINITY,
        f32::NEG_INFINITY,
        16777216.0,
        -16777216.0,
        -0.0,
        f32::from_bits(1),
        f32::MIN_POSITIVE,
        f32::MAX,
        -f32::MAX,
    ];
    let mut null = Bitmap::all_set(config.rows);
    let mut missing = Bitmap::all_set(config.rows);
    let mut active = Bitmap::all_unset(config.rows);
    let data: Vec<_> = (0..config.rows)
        .map(|row| {
            if config.nullable {
                if row % 13 == 0 {
                    missing.unset(row);
                } else if row % 11 == 0 {
                    null.unset(row);
                }
            }
            if row % 100 < config.active_percent as usize {
                active.set(row);
            }
            special.get(row).copied().unwrap_or((row % 1021) as f32 * 0.25 - 128.0)
        })
        .collect();
    ColumnBatch {
        columns: vec![TypedColumn::Float32 {
            data: PaddedVec::from_vec(data),
            null,
            missing,
        }],
        names: vec!["v".into()],
        selection: SelectionVector::Bitmap(active),
        len: config.rows,
    }
}

fn run_kernel(
    name: &str,
    batch: &ColumnBatch,
    bound: &mut BoundExpression,
    scalar: &ResolvedFunction,
    chain: usize,
) -> ProbeResult<TypedColumn> {
    if name == "typed_f32" {
        return Ok(run_typed(batch, chain));
    }
    let mut values = vec![Value::Missing; batch.len];
    for (row, output) in values.iter_mut().enumerate() {
        if !batch.selection.is_active(row, batch.len) {
            continue;
        }
        *output = match name {
            "bound_expression" => bound.evaluate(batch, row)?,
            "registered_scalar" => {
                let mut value = BatchToRowAdapter::extract_value(&batch.columns[0], row);
                for _ in 0..chain {
                    value = scalar.call(&[value, Value::Float(OrderedFloat(0.5))])?;
                }
                value
            }
            _ => return Err("unknown expression probe kernel".into()),
        };
    }
    Ok(typed_column(values))
}

fn run_typed(batch: &ColumnBatch, chain: usize) -> TypedColumn {
    let TypedColumn::Float32 {
        data,
        null: input_null,
        missing: input_missing,
    } = &batch.columns[0]
    else {
        unreachable!("fixed Float32 fixture")
    };
    let mut output = PaddedVecBuilder::with_capacity(batch.len + 8);
    let mut null = Bitmap::all_set(batch.len);
    let mut missing = Bitmap::all_set(batch.len);
    for row in 0..batch.len {
        if !batch.selection.is_active(row, batch.len) || !input_missing.is_set(row) {
            missing.unset(row);
            output.push(0.0);
        } else if !input_null.is_set(row) {
            null.unset(row);
            output.push(0.0);
        } else {
            let mut value = data[row];
            for _ in 0..chain {
                value += 0.5_f32;
            }
            output.push(value);
        }
    }
    TypedColumn::Float32 {
        data: output.seal(),
        null,
        missing,
    }
}

fn oracle_value(batch: &ColumnBatch, row: usize, chain: usize) -> Value {
    if !batch.selection.is_active(row, batch.len) {
        return Value::Missing;
    }
    let TypedColumn::Float32 { data, null, missing } = &batch.columns[0] else {
        unreachable!()
    };
    if !missing.is_set(row) {
        return Value::Missing;
    }
    if !null.is_set(row) {
        return Value::Null;
    }
    // Compute at f64 precision and round after EVERY step; a collapsed
    // +chain*0.5 is observably different at 2^24.
    let mut value = data[row];
    for _ in 0..chain {
        value = (f64::from(value) + 0.5) as f32;
    }
    Value::Float(OrderedFloat(value))
}

fn validate_output(batch: &ColumnBatch, output: &TypedColumn, chain: usize) -> ProbeResult<()> {
    for row in 0..batch.len {
        let expected = oracle_value(batch, row, chain);
        let actual = BatchToRowAdapter::extract_value(output, row);
        let equal = match (&expected, &actual) {
            (Value::Float(left), Value::Float(right)) => {
                (left.is_nan() && right.is_nan()) || left.to_bits() == right.to_bits()
            }
            _ => actual == expected,
        };
        if !equal {
            return Err(format!("row {row}: expected {expected:?}, got {actual:?}").into());
        }
    }
    Ok(())
}

fn summarize(config: &ExpressionProbeConfig, values: impl Iterator<Item = Value>) -> serde_json::Value {
    let mut active = 0;
    let mut nulls = 0;
    let mut missing = 0;
    let mut numbers = 0;
    let mut sum = 0.0_f64;
    for (row, value) in values.enumerate() {
        if row % 100 >= config.active_percent as usize {
            continue;
        }
        active += 1;
        match value {
            Value::Null => nulls += 1,
            Value::Missing => missing += 1,
            Value::Float(value) => {
                numbers += 1;
                sum += f64::from(value.0);
            }
            _ => unreachable!("fixed Float32 output"),
        }
    }
    let kind = if numbers == 0 {
        "empty"
    } else if sum.is_nan() {
        "nan"
    } else if sum == f64::INFINITY {
        "positive_infinity"
    } else if sum == f64::NEG_INFINITY {
        "negative_infinity"
    } else {
        "finite"
    };
    serde_json::json!({"active_rows": active, "null_rows": nulls, "missing_rows": missing, "numeric_rows": numbers,
        "sum_kind": kind, "sum_f64": (numbers > 0 && sum.is_finite()).then_some(sum)})
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config() -> ExpressionProbeConfig {
        ExpressionProbeConfig {
            rows: 257,
            chain_length: 1,
            nullable: true,
            active_percent: 100,
            reverse: false,
        }
    }

    #[test]
    fn expression_probe_checks_every_kernel_with_null_missing_and_selection() {
        for chain_length in [1, 16] {
            for active_percent in [0, 1, 50, 100] {
                let report = profile_expressions(ExpressionProbeConfig {
                    chain_length,
                    active_percent,
                    ..config()
                })
                .unwrap();
                assert_eq!(report["validation"], "passed");
                assert_eq!(report["kernels"].as_array().unwrap().len(), 3);
                let active = (0..257).filter(|row| row % 100 < active_percent as usize).count();
                assert_eq!(report["oracle"]["active_rows"], active);
                for kernel in report["kernels"].as_array().unwrap() {
                    assert_eq!(kernel["checked_rows"], 257);
                    assert_eq!(kernel["counts_and_sum"], report["oracle"]);
                }
            }
        }
    }

    #[test]
    fn expression_probe_rejects_invalid_or_unbounded_configuration() {
        for invalid in [
            ExpressionProbeConfig { rows: 0, ..config() },
            ExpressionProbeConfig {
                rows: 2_000_001,
                ..config()
            },
            ExpressionProbeConfig {
                chain_length: 2,
                ..config()
            },
            ExpressionProbeConfig {
                active_percent: 101,
                ..config()
            },
        ] {
            assert!(profile_expressions(invalid).is_err());
        }
    }

    #[test]
    fn expression_probe_checks_rounding_and_every_row_including_masked_rows() {
        let config = ExpressionProbeConfig {
            nullable: false,
            ..config()
        };
        let batch = input(&config);
        assert_eq!(oracle_value(&batch, 3, 16), Value::Float(OrderedFloat(16777216.0)));
        assert_ne!(16777216.0_f32 + 8.0, 16777216.0_f32);
        let mut output = run_typed(&batch, 16);
        validate_output(&batch, &output, 16).unwrap();
        if let TypedColumn::Float32 { data, .. } = &mut output {
            data.inner[100] += 1.0;
            data.inner[101] -= 1.0;
        }
        assert!(
            validate_output(&batch, &output, 16).is_err(),
            "balanced errors cannot evade per-row checks"
        );
        let batch = input(&ExpressionProbeConfig {
            active_percent: 0,
            ..config
        });
        let mut output = run_typed(&batch, 1);
        if let TypedColumn::Float32 { missing, .. } = &mut output {
            missing.set(0);
        }
        assert!(validate_output(&batch, &output, 1).is_err());
    }

    #[test]
    fn expression_probe_does_not_treat_custom_plus_as_the_builtin() {
        use crate::functions::{Arity, FunctionDef, FunctionRegistry, NullHandling};
        let mut registry = FunctionRegistry::new();
        registry
            .register(FunctionDef {
                name: "plus".into(),
                arity: Arity::Exact(2),
                null_handling: NullHandling::Custom,
                func: Box::new(|_| Ok(Value::Float(OrderedFloat(42.0)))),
            })
            .unwrap();
        let config = config();
        let batch = input(&config);
        let expression = Expression::Function(
            "plus".into(),
            vec![
                Named::Expression(
                    Expression::Variable(PathExpr::new(vec![PathSegment::AttrName("v".into())])),
                    None,
                ),
                Named::Expression(Expression::Constant(Value::Float(OrderedFloat(0.5))), None),
            ],
        );
        let mut bound = BoundExpression::bind(
            &expression,
            &BatchSchema {
                names: batch.names.clone(),
                types: vec![ColumnType::Float32],
            },
            &Variables::new(),
            &registry,
        );
        let output = run_kernel(
            "bound_expression",
            &batch,
            &mut bound,
            &registry.resolve("plus").unwrap(),
            1,
        )
        .unwrap();
        assert!(
            validate_output(&batch, &output, 1).is_err(),
            "a custom function name is not an intrinsic proof"
        );
    }
}
