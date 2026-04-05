use crate::common::types::Value;
use crate::execution::types::ExpressionError;
use crate::functions::registry::{FunctionDef, FunctionRegistry, Arity, NullHandling, RegistryError};
use ordered_float::OrderedFloat;

pub fn register(registry: &mut FunctionRegistry) -> Result<(), RegistryError> {
    // ── Existing operator-style functions ──

    registry.register(FunctionDef {
        name: "Plus".to_string(),
        arity: Arity::Exact(2),
        null_handling: NullHandling::Propagate,
        func: Box::new(|args| match (&args[0], &args[1]) {
            (Value::Int(a), Value::Int(b)) => Ok(Value::Int(a + b)),
            (Value::Float(a), Value::Float(b)) => Ok(Value::Float(OrderedFloat(a.into_inner() + b.into_inner()))),
            (Value::Int(a), Value::Float(b)) => Ok(Value::Float(OrderedFloat(*a as f32 + b.into_inner()))),
            (Value::Float(a), Value::Int(b)) => Ok(Value::Float(OrderedFloat(a.into_inner() + *b as f32))),
            _ => Err(ExpressionError::InvalidArguments),
        }),
    })?;

    registry.register(FunctionDef {
        name: "Minus".to_string(),
        arity: Arity::Exact(2),
        null_handling: NullHandling::Propagate,
        func: Box::new(|args| match (&args[0], &args[1]) {
            (Value::Int(a), Value::Int(b)) => Ok(Value::Int(a - b)),
            (Value::Float(a), Value::Float(b)) => Ok(Value::Float(OrderedFloat(a.into_inner() - b.into_inner()))),
            (Value::Int(a), Value::Float(b)) => Ok(Value::Float(OrderedFloat(*a as f32 - b.into_inner()))),
            (Value::Float(a), Value::Int(b)) => Ok(Value::Float(OrderedFloat(a.into_inner() - *b as f32))),
            _ => Err(ExpressionError::InvalidArguments),
        }),
    })?;

    registry.register(FunctionDef {
        name: "Times".to_string(),
        arity: Arity::Exact(2),
        null_handling: NullHandling::Propagate,
        func: Box::new(|args| match (&args[0], &args[1]) {
            (Value::Int(a), Value::Int(b)) => Ok(Value::Int(a * b)),
            (Value::Float(a), Value::Float(b)) => Ok(Value::Float(OrderedFloat(a.into_inner() * b.into_inner()))),
            (Value::Int(a), Value::Float(b)) => Ok(Value::Float(OrderedFloat(*a as f32 * b.into_inner()))),
            (Value::Float(a), Value::Int(b)) => Ok(Value::Float(OrderedFloat(a.into_inner() * *b as f32))),
            _ => Err(ExpressionError::InvalidArguments),
        }),
    })?;

    registry.register(FunctionDef {
        name: "Divide".to_string(),
        arity: Arity::Exact(2),
        null_handling: NullHandling::Propagate,
        func: Box::new(|args| match (&args[0], &args[1]) {
            (Value::Int(a), Value::Int(b)) => {
                if *b == 0 { Ok(Value::Null) } else { Ok(Value::Int(a / b)) }
            }
            (Value::Float(a), Value::Float(b)) => Ok(Value::Float(OrderedFloat(a.into_inner() / b.into_inner()))),
            (Value::Int(a), Value::Float(b)) => Ok(Value::Float(OrderedFloat(*a as f32 / b.into_inner()))),
            (Value::Float(a), Value::Int(b)) => Ok(Value::Float(OrderedFloat(a.into_inner() / *b as f32))),
            _ => Err(ExpressionError::InvalidArguments),
        }),
    })?;

    // ── Basic math (Arity::Exact(1)) ──

    // abs
    registry.register(FunctionDef {
        name: "abs".to_string(),
        arity: Arity::Exact(1),
        null_handling: NullHandling::Propagate,
        func: Box::new(|args| match &args[0] {
            Value::Int(v) => Ok(Value::Int(v.abs())),
            Value::Float(v) => Ok(Value::Float(OrderedFloat(v.into_inner().abs()))),
            _ => Err(ExpressionError::InvalidArguments),
        }),
    })?;

    // ceil
    registry.register(FunctionDef {
        name: "ceil".to_string(),
        arity: Arity::Exact(1),
        null_handling: NullHandling::Propagate,
        func: Box::new(|args| match &args[0] {
            Value::Float(v) => Ok(Value::Int(v.into_inner().ceil() as i32)),
            Value::Int(v) => Ok(Value::Int(*v)),
            _ => Err(ExpressionError::InvalidArguments),
        }),
    })?;

    // ceiling (alias for ceil)
    registry.register(FunctionDef {
        name: "ceiling".to_string(),
        arity: Arity::Exact(1),
        null_handling: NullHandling::Propagate,
        func: Box::new(|args| match &args[0] {
            Value::Float(v) => Ok(Value::Int(v.into_inner().ceil() as i32)),
            Value::Int(v) => Ok(Value::Int(*v)),
            _ => Err(ExpressionError::InvalidArguments),
        }),
    })?;

    // floor
    registry.register(FunctionDef {
        name: "floor".to_string(),
        arity: Arity::Exact(1),
        null_handling: NullHandling::Propagate,
        func: Box::new(|args| match &args[0] {
            Value::Float(v) => Ok(Value::Int(v.into_inner().floor() as i32)),
            Value::Int(v) => Ok(Value::Int(*v)),
            _ => Err(ExpressionError::InvalidArguments),
        }),
    })?;

    // round: 1 arg → round to integer, 2 args → round to N decimals → Float
    registry.register(FunctionDef {
        name: "round".to_string(),
        arity: Arity::Range(1, 2),
        null_handling: NullHandling::Propagate,
        func: Box::new(|args| {
            if args.len() == 1 {
                match &args[0] {
                    Value::Float(v) => Ok(Value::Int(v.into_inner().round() as i32)),
                    Value::Int(v) => Ok(Value::Int(*v)),
                    _ => Err(ExpressionError::InvalidArguments),
                }
            } else {
                // 2-arg form: round(value, decimals) → Float
                let val = match &args[0] {
                    Value::Float(v) => v.into_inner(),
                    Value::Int(v) => *v as f32,
                    _ => return Err(ExpressionError::InvalidArguments),
                };
                let decimals = match &args[1] {
                    Value::Int(d) => *d,
                    _ => return Err(ExpressionError::InvalidArguments),
                };
                let factor = 10.0_f32.powi(decimals);
                Ok(Value::Float(OrderedFloat((val * factor).round() / factor)))
            }
        }),
    })?;

    // sqrt
    registry.register(FunctionDef {
        name: "sqrt".to_string(),
        arity: Arity::Exact(1),
        null_handling: NullHandling::Propagate,
        func: Box::new(|args| match &args[0] {
            Value::Float(v) => Ok(Value::Float(OrderedFloat(v.into_inner().sqrt()))),
            Value::Int(v) => Ok(Value::Float(OrderedFloat((*v as f32).sqrt()))),
            _ => Err(ExpressionError::InvalidArguments),
        }),
    })?;

    // sign
    registry.register(FunctionDef {
        name: "sign".to_string(),
        arity: Arity::Exact(1),
        null_handling: NullHandling::Propagate,
        func: Box::new(|args| match &args[0] {
            Value::Int(v) => Ok(Value::Int(if *v > 0 { 1 } else if *v < 0 { -1 } else { 0 })),
            Value::Float(v) => {
                let f = v.into_inner();
                Ok(Value::Float(OrderedFloat(if f > 0.0 { 1.0 } else if f < 0.0 { -1.0 } else { 0.0 })))
            }
            _ => Err(ExpressionError::InvalidArguments),
        }),
    })?;

    // truncate
    registry.register(FunctionDef {
        name: "truncate".to_string(),
        arity: Arity::Exact(1),
        null_handling: NullHandling::Propagate,
        func: Box::new(|args| match &args[0] {
            Value::Float(v) => Ok(Value::Int(v.into_inner().trunc() as i32)),
            Value::Int(v) => Ok(Value::Int(*v)),
            _ => Err(ExpressionError::InvalidArguments),
        }),
    })?;

    // trunc (alias for truncate)
    registry.register(FunctionDef {
        name: "trunc".to_string(),
        arity: Arity::Exact(1),
        null_handling: NullHandling::Propagate,
        func: Box::new(|args| match &args[0] {
            Value::Float(v) => Ok(Value::Int(v.into_inner().trunc() as i32)),
            Value::Int(v) => Ok(Value::Int(*v)),
            _ => Err(ExpressionError::InvalidArguments),
        }),
    })?;

    // ln
    registry.register(FunctionDef {
        name: "ln".to_string(),
        arity: Arity::Exact(1),
        null_handling: NullHandling::Propagate,
        func: Box::new(|args| match &args[0] {
            Value::Float(v) => Ok(Value::Float(OrderedFloat(v.into_inner().ln()))),
            Value::Int(v) => Ok(Value::Float(OrderedFloat((*v as f32).ln()))),
            _ => Err(ExpressionError::InvalidArguments),
        }),
    })?;

    // log2
    registry.register(FunctionDef {
        name: "log2".to_string(),
        arity: Arity::Exact(1),
        null_handling: NullHandling::Propagate,
        func: Box::new(|args| match &args[0] {
            Value::Float(v) => Ok(Value::Float(OrderedFloat(v.into_inner().log2()))),
            Value::Int(v) => Ok(Value::Float(OrderedFloat((*v as f32).log2()))),
            _ => Err(ExpressionError::InvalidArguments),
        }),
    })?;

    // log10
    registry.register(FunctionDef {
        name: "log10".to_string(),
        arity: Arity::Exact(1),
        null_handling: NullHandling::Propagate,
        func: Box::new(|args| match &args[0] {
            Value::Float(v) => Ok(Value::Float(OrderedFloat(v.into_inner().log10()))),
            Value::Int(v) => Ok(Value::Float(OrderedFloat((*v as f32).log10()))),
            _ => Err(ExpressionError::InvalidArguments),
        }),
    })?;

    // exp
    registry.register(FunctionDef {
        name: "exp".to_string(),
        arity: Arity::Exact(1),
        null_handling: NullHandling::Propagate,
        func: Box::new(|args| match &args[0] {
            Value::Float(v) => Ok(Value::Float(OrderedFloat(v.into_inner().exp()))),
            Value::Int(v) => Ok(Value::Float(OrderedFloat((*v as f32).exp()))),
            _ => Err(ExpressionError::InvalidArguments),
        }),
    })?;

    // ── Two-arg math (Arity::Exact(2)) ──

    // power
    registry.register(FunctionDef {
        name: "power".to_string(),
        arity: Arity::Exact(2),
        null_handling: NullHandling::Propagate,
        func: Box::new(|args| {
            let base = match &args[0] {
                Value::Float(v) => v.into_inner(),
                Value::Int(v) => *v as f32,
                _ => return Err(ExpressionError::InvalidArguments),
            };
            let exp = match &args[1] {
                Value::Float(v) => v.into_inner(),
                Value::Int(v) => *v as f32,
                _ => return Err(ExpressionError::InvalidArguments),
            };
            Ok(Value::Float(OrderedFloat(base.powf(exp))))
        }),
    })?;

    // pow (alias for power)
    registry.register(FunctionDef {
        name: "pow".to_string(),
        arity: Arity::Exact(2),
        null_handling: NullHandling::Propagate,
        func: Box::new(|args| {
            let base = match &args[0] {
                Value::Float(v) => v.into_inner(),
                Value::Int(v) => *v as f32,
                _ => return Err(ExpressionError::InvalidArguments),
            };
            let exp = match &args[1] {
                Value::Float(v) => v.into_inner(),
                Value::Int(v) => *v as f32,
                _ => return Err(ExpressionError::InvalidArguments),
            };
            Ok(Value::Float(OrderedFloat(base.powf(exp))))
        }),
    })?;

    // mod
    registry.register(FunctionDef {
        name: "mod".to_string(),
        arity: Arity::Exact(2),
        null_handling: NullHandling::Propagate,
        func: Box::new(|args| match (&args[0], &args[1]) {
            (Value::Int(a), Value::Int(b)) => {
                if *b == 0 { Ok(Value::Null) } else { Ok(Value::Int(a % b)) }
            }
            (Value::Float(a), Value::Float(b)) => Ok(Value::Float(OrderedFloat(a.into_inner() % b.into_inner()))),
            (Value::Int(a), Value::Float(b)) => Ok(Value::Float(OrderedFloat(*a as f32 % b.into_inner()))),
            (Value::Float(a), Value::Int(b)) => Ok(Value::Float(OrderedFloat(a.into_inner() % *b as f32))),
            _ => Err(ExpressionError::InvalidArguments),
        }),
    })?;

    // modulus (alias for mod)
    registry.register(FunctionDef {
        name: "modulus".to_string(),
        arity: Arity::Exact(2),
        null_handling: NullHandling::Propagate,
        func: Box::new(|args| match (&args[0], &args[1]) {
            (Value::Int(a), Value::Int(b)) => {
                if *b == 0 { Ok(Value::Null) } else { Ok(Value::Int(a % b)) }
            }
            (Value::Float(a), Value::Float(b)) => Ok(Value::Float(OrderedFloat(a.into_inner() % b.into_inner()))),
            (Value::Int(a), Value::Float(b)) => Ok(Value::Float(OrderedFloat(*a as f32 % b.into_inner()))),
            (Value::Float(a), Value::Int(b)) => Ok(Value::Float(OrderedFloat(a.into_inner() % *b as f32))),
            _ => Err(ExpressionError::InvalidArguments),
        }),
    })?;

    // atan2
    registry.register(FunctionDef {
        name: "atan2".to_string(),
        arity: Arity::Exact(2),
        null_handling: NullHandling::Propagate,
        func: Box::new(|args| {
            let y = match &args[0] {
                Value::Float(v) => v.into_inner(),
                Value::Int(v) => *v as f32,
                _ => return Err(ExpressionError::InvalidArguments),
            };
            let x = match &args[1] {
                Value::Float(v) => v.into_inner(),
                Value::Int(v) => *v as f32,
                _ => return Err(ExpressionError::InvalidArguments),
            };
            Ok(Value::Float(OrderedFloat(y.atan2(x))))
        }),
    })?;

    // ── Constants (Arity::Exact(0)) ──

    // pi
    registry.register(FunctionDef {
        name: "pi".to_string(),
        arity: Arity::Exact(0),
        null_handling: NullHandling::Propagate,
        func: Box::new(|_args| Ok(Value::Float(OrderedFloat(std::f32::consts::PI)))),
    })?;

    // e
    registry.register(FunctionDef {
        name: "e".to_string(),
        arity: Arity::Exact(0),
        null_handling: NullHandling::Propagate,
        func: Box::new(|_args| Ok(Value::Float(OrderedFloat(std::f32::consts::E)))),
    })?;

    // infinity
    registry.register(FunctionDef {
        name: "infinity".to_string(),
        arity: Arity::Exact(0),
        null_handling: NullHandling::Propagate,
        func: Box::new(|_args| Ok(Value::Float(OrderedFloat(f32::INFINITY)))),
    })?;

    // nan
    registry.register(FunctionDef {
        name: "nan".to_string(),
        arity: Arity::Exact(0),
        null_handling: NullHandling::Propagate,
        func: Box::new(|_args| Ok(Value::Float(OrderedFloat(f32::NAN)))),
    })?;

    // ── Predicates (Arity::Exact(1), return Boolean) ──

    // is_nan
    registry.register(FunctionDef {
        name: "is_nan".to_string(),
        arity: Arity::Exact(1),
        null_handling: NullHandling::Propagate,
        func: Box::new(|args| match &args[0] {
            Value::Float(v) => Ok(Value::Boolean(v.into_inner().is_nan())),
            _ => Err(ExpressionError::InvalidArguments),
        }),
    })?;

    // is_finite
    registry.register(FunctionDef {
        name: "is_finite".to_string(),
        arity: Arity::Exact(1),
        null_handling: NullHandling::Propagate,
        func: Box::new(|args| match &args[0] {
            Value::Float(v) => Ok(Value::Boolean(v.into_inner().is_finite())),
            _ => Err(ExpressionError::InvalidArguments),
        }),
    })?;

    // is_infinite
    registry.register(FunctionDef {
        name: "is_infinite".to_string(),
        arity: Arity::Exact(1),
        null_handling: NullHandling::Propagate,
        func: Box::new(|args| match &args[0] {
            Value::Float(v) => {
                let f = v.into_inner();
                Ok(Value::Boolean(!f.is_finite() && !f.is_nan()))
            }
            _ => Err(ExpressionError::InvalidArguments),
        }),
    })?;

    // ── Trig (Arity::Exact(1), all work in radians) ──

    // radians: degrees → Float
    registry.register(FunctionDef {
        name: "radians".to_string(),
        arity: Arity::Exact(1),
        null_handling: NullHandling::Propagate,
        func: Box::new(|args| match &args[0] {
            Value::Float(v) => Ok(Value::Float(OrderedFloat(v.into_inner() * std::f32::consts::PI / 180.0))),
            Value::Int(v) => Ok(Value::Float(OrderedFloat(*v as f32 * std::f32::consts::PI / 180.0))),
            _ => Err(ExpressionError::InvalidArguments),
        }),
    })?;

    // degrees: radians → Float
    registry.register(FunctionDef {
        name: "degrees".to_string(),
        arity: Arity::Exact(1),
        null_handling: NullHandling::Propagate,
        func: Box::new(|args| match &args[0] {
            Value::Float(v) => Ok(Value::Float(OrderedFloat(v.into_inner() * 180.0 / std::f32::consts::PI))),
            Value::Int(v) => Ok(Value::Float(OrderedFloat(*v as f32 * 180.0 / std::f32::consts::PI))),
            _ => Err(ExpressionError::InvalidArguments),
        }),
    })?;

    // sin
    registry.register(FunctionDef {
        name: "sin".to_string(),
        arity: Arity::Exact(1),
        null_handling: NullHandling::Propagate,
        func: Box::new(|args| match &args[0] {
            Value::Float(v) => Ok(Value::Float(OrderedFloat(v.into_inner().sin()))),
            Value::Int(v) => Ok(Value::Float(OrderedFloat((*v as f32).sin()))),
            _ => Err(ExpressionError::InvalidArguments),
        }),
    })?;

    // cos
    registry.register(FunctionDef {
        name: "cos".to_string(),
        arity: Arity::Exact(1),
        null_handling: NullHandling::Propagate,
        func: Box::new(|args| match &args[0] {
            Value::Float(v) => Ok(Value::Float(OrderedFloat(v.into_inner().cos()))),
            Value::Int(v) => Ok(Value::Float(OrderedFloat((*v as f32).cos()))),
            _ => Err(ExpressionError::InvalidArguments),
        }),
    })?;

    // tan
    registry.register(FunctionDef {
        name: "tan".to_string(),
        arity: Arity::Exact(1),
        null_handling: NullHandling::Propagate,
        func: Box::new(|args| match &args[0] {
            Value::Float(v) => Ok(Value::Float(OrderedFloat(v.into_inner().tan()))),
            Value::Int(v) => Ok(Value::Float(OrderedFloat((*v as f32).tan()))),
            _ => Err(ExpressionError::InvalidArguments),
        }),
    })?;

    // asin
    registry.register(FunctionDef {
        name: "asin".to_string(),
        arity: Arity::Exact(1),
        null_handling: NullHandling::Propagate,
        func: Box::new(|args| match &args[0] {
            Value::Float(v) => Ok(Value::Float(OrderedFloat(v.into_inner().asin()))),
            Value::Int(v) => Ok(Value::Float(OrderedFloat((*v as f32).asin()))),
            _ => Err(ExpressionError::InvalidArguments),
        }),
    })?;

    // acos
    registry.register(FunctionDef {
        name: "acos".to_string(),
        arity: Arity::Exact(1),
        null_handling: NullHandling::Propagate,
        func: Box::new(|args| match &args[0] {
            Value::Float(v) => Ok(Value::Float(OrderedFloat(v.into_inner().acos()))),
            Value::Int(v) => Ok(Value::Float(OrderedFloat((*v as f32).acos()))),
            _ => Err(ExpressionError::InvalidArguments),
        }),
    })?;

    // atan
    registry.register(FunctionDef {
        name: "atan".to_string(),
        arity: Arity::Exact(1),
        null_handling: NullHandling::Propagate,
        func: Box::new(|args| match &args[0] {
            Value::Float(v) => Ok(Value::Float(OrderedFloat(v.into_inner().atan()))),
            Value::Int(v) => Ok(Value::Float(OrderedFloat((*v as f32).atan()))),
            _ => Err(ExpressionError::InvalidArguments),
        }),
    })?;

    // cosh
    registry.register(FunctionDef {
        name: "cosh".to_string(),
        arity: Arity::Exact(1),
        null_handling: NullHandling::Propagate,
        func: Box::new(|args| match &args[0] {
            Value::Float(v) => Ok(Value::Float(OrderedFloat(v.into_inner().cosh()))),
            Value::Int(v) => Ok(Value::Float(OrderedFloat((*v as f32).cosh()))),
            _ => Err(ExpressionError::InvalidArguments),
        }),
    })?;

    // tanh
    registry.register(FunctionDef {
        name: "tanh".to_string(),
        arity: Arity::Exact(1),
        null_handling: NullHandling::Propagate,
        func: Box::new(|args| match &args[0] {
            Value::Float(v) => Ok(Value::Float(OrderedFloat(v.into_inner().tanh()))),
            Value::Int(v) => Ok(Value::Float(OrderedFloat((*v as f32).tanh()))),
            _ => Err(ExpressionError::InvalidArguments),
        }),
    })?;

    // ── Random (Arity::Exact(0)) ──

    // rand
    registry.register(FunctionDef {
        name: "rand".to_string(),
        arity: Arity::Exact(0),
        null_handling: NullHandling::Propagate,
        func: Box::new(|_args| {
            let nanos = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .subsec_nanos();
            Ok(Value::Float(OrderedFloat(nanos as f32 / 1_000_000_000.0)))
        }),
    })?;

    // random (alias for rand)
    registry.register(FunctionDef {
        name: "random".to_string(),
        arity: Arity::Exact(0),
        null_handling: NullHandling::Propagate,
        func: Box::new(|_args| {
            let nanos = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .subsec_nanos();
            Ok(Value::Float(OrderedFloat(nanos as f32 / 1_000_000_000.0)))
        }),
    })?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::types::Value;
    use crate::functions::registry::FunctionRegistry;
    use ordered_float::OrderedFloat;

    fn make_registry() -> FunctionRegistry {
        let mut r = FunctionRegistry::new();
        register(&mut r).unwrap();
        r
    }

    // ── Existing operator tests ──

    #[test]
    fn test_plus_int() {
        let r = make_registry();
        assert_eq!(r.call("Plus", &[Value::Int(1), Value::Int(2)]), Ok(Value::Int(3)));
    }

    #[test]
    fn test_plus_float() {
        let r = make_registry();
        let result = r.call("Plus", &[Value::Float(OrderedFloat(1.5)), Value::Float(OrderedFloat(2.5))]);
        assert_eq!(result, Ok(Value::Float(OrderedFloat(4.0))));
    }

    #[test]
    fn test_plus_int_float_coercion() {
        let r = make_registry();
        let result = r.call("Plus", &[Value::Int(1), Value::Float(OrderedFloat(2.5))]);
        assert_eq!(result, Ok(Value::Float(OrderedFloat(3.5))));
    }

    #[test]
    fn test_plus_null_propagation() {
        let r = make_registry();
        assert_eq!(r.call("Plus", &[Value::Null, Value::Int(1)]), Ok(Value::Null));
        assert_eq!(r.call("Plus", &[Value::Missing, Value::Null]), Ok(Value::Missing));
    }

    #[test]
    fn test_divide_by_zero() {
        let r = make_registry();
        assert_eq!(r.call("Divide", &[Value::Int(1), Value::Int(0)]), Ok(Value::Null));
    }

    #[test]
    fn test_minus_int() {
        let r = make_registry();
        assert_eq!(r.call("Minus", &[Value::Int(5), Value::Int(3)]), Ok(Value::Int(2)));
    }

    #[test]
    fn test_times_int() {
        let r = make_registry();
        assert_eq!(r.call("Times", &[Value::Int(3), Value::Int(4)]), Ok(Value::Int(12)));
    }

    // ── New arithmetic function tests ──

    #[test]
    fn test_abs_int() {
        let r = make_registry();
        assert_eq!(r.call("abs", &[Value::Int(-5)]), Ok(Value::Int(5)));
    }

    #[test]
    fn test_abs_float() {
        let r = make_registry();
        assert_eq!(r.call("abs", &[Value::Float(OrderedFloat(-3.14))]), Ok(Value::Float(OrderedFloat(3.14))));
    }

    #[test]
    fn test_ceil() {
        let r = make_registry();
        assert_eq!(r.call("ceil", &[Value::Float(OrderedFloat(2.3))]), Ok(Value::Int(3)));
    }

    #[test]
    fn test_ceiling_alias() {
        let r = make_registry();
        assert_eq!(r.call("ceiling", &[Value::Float(OrderedFloat(2.3))]), Ok(Value::Int(3)));
    }

    #[test]
    fn test_ceil_int_passthrough() {
        let r = make_registry();
        assert_eq!(r.call("ceil", &[Value::Int(5)]), Ok(Value::Int(5)));
    }

    #[test]
    fn test_floor() {
        let r = make_registry();
        assert_eq!(r.call("floor", &[Value::Float(OrderedFloat(2.7))]), Ok(Value::Int(2)));
    }

    #[test]
    fn test_floor_int_passthrough() {
        let r = make_registry();
        assert_eq!(r.call("floor", &[Value::Int(5)]), Ok(Value::Int(5)));
    }

    #[test]
    fn test_round() {
        let r = make_registry();
        assert_eq!(r.call("round", &[Value::Float(OrderedFloat(2.5))]), Ok(Value::Int(3)));
    }

    #[test]
    fn test_round_down() {
        let r = make_registry();
        assert_eq!(r.call("round", &[Value::Float(OrderedFloat(2.3))]), Ok(Value::Int(2)));
    }

    #[test]
    fn test_round_two_args() {
        let r = make_registry();
        assert_eq!(
            r.call("round", &[Value::Float(OrderedFloat(3.14159)), Value::Int(2)]),
            Ok(Value::Float(OrderedFloat(3.14)))
        );
    }

    #[test]
    fn test_round_int_passthrough() {
        let r = make_registry();
        assert_eq!(r.call("round", &[Value::Int(5)]), Ok(Value::Int(5)));
    }

    #[test]
    fn test_power() {
        let r = make_registry();
        assert_eq!(r.call("power", &[Value::Int(2), Value::Int(3)]), Ok(Value::Float(OrderedFloat(8.0))));
    }

    #[test]
    fn test_pow_alias() {
        let r = make_registry();
        assert_eq!(r.call("pow", &[Value::Int(2), Value::Int(3)]), Ok(Value::Float(OrderedFloat(8.0))));
    }

    #[test]
    fn test_power_float() {
        let r = make_registry();
        assert_eq!(
            r.call("power", &[Value::Float(OrderedFloat(2.0)), Value::Float(OrderedFloat(0.5))]),
            Ok(Value::Float(OrderedFloat(2.0_f32.powf(0.5))))
        );
    }

    #[test]
    fn test_mod_int() {
        let r = make_registry();
        assert_eq!(r.call("mod", &[Value::Int(10), Value::Int(3)]), Ok(Value::Int(1)));
    }

    #[test]
    fn test_mod_by_zero() {
        let r = make_registry();
        assert_eq!(r.call("mod", &[Value::Int(10), Value::Int(0)]), Ok(Value::Null));
    }

    #[test]
    fn test_modulus_alias() {
        let r = make_registry();
        assert_eq!(r.call("modulus", &[Value::Int(10), Value::Int(3)]), Ok(Value::Int(1)));
    }

    #[test]
    fn test_sqrt() {
        let r = make_registry();
        assert_eq!(r.call("sqrt", &[Value::Float(OrderedFloat(9.0))]), Ok(Value::Float(OrderedFloat(3.0))));
    }

    #[test]
    fn test_sqrt_int() {
        let r = make_registry();
        assert_eq!(r.call("sqrt", &[Value::Int(16)]), Ok(Value::Float(OrderedFloat(4.0))));
    }

    #[test]
    fn test_sign() {
        let r = make_registry();
        assert_eq!(r.call("sign", &[Value::Int(-5)]), Ok(Value::Int(-1)));
        assert_eq!(r.call("sign", &[Value::Int(0)]), Ok(Value::Int(0)));
        assert_eq!(r.call("sign", &[Value::Int(5)]), Ok(Value::Int(1)));
    }

    #[test]
    fn test_sign_float() {
        let r = make_registry();
        assert_eq!(r.call("sign", &[Value::Float(OrderedFloat(-2.5))]), Ok(Value::Float(OrderedFloat(-1.0))));
        assert_eq!(r.call("sign", &[Value::Float(OrderedFloat(0.0))]), Ok(Value::Float(OrderedFloat(0.0))));
        assert_eq!(r.call("sign", &[Value::Float(OrderedFloat(2.5))]), Ok(Value::Float(OrderedFloat(1.0))));
    }

    #[test]
    fn test_truncate() {
        let r = make_registry();
        assert_eq!(r.call("truncate", &[Value::Float(OrderedFloat(2.9))]), Ok(Value::Int(2)));
        assert_eq!(r.call("truncate", &[Value::Float(OrderedFloat(-2.9))]), Ok(Value::Int(-2)));
    }

    #[test]
    fn test_trunc_alias() {
        let r = make_registry();
        assert_eq!(r.call("trunc", &[Value::Float(OrderedFloat(2.9))]), Ok(Value::Int(2)));
    }

    #[test]
    fn test_ln() {
        let r = make_registry();
        assert_eq!(r.call("ln", &[Value::Float(OrderedFloat(1.0))]), Ok(Value::Float(OrderedFloat(0.0))));
    }

    #[test]
    fn test_ln_e() {
        let r = make_registry();
        let result = r.call("ln", &[Value::Float(OrderedFloat(std::f32::consts::E))]);
        match result {
            Ok(Value::Float(v)) => {
                let diff = (v.into_inner() - 1.0).abs();
                assert!(diff < 1e-6, "ln(e) should be ~1.0, got {}", v.into_inner());
            }
            other => panic!("Expected Float, got {:?}", other),
        }
    }

    #[test]
    fn test_log2() {
        let r = make_registry();
        assert_eq!(r.call("log2", &[Value::Float(OrderedFloat(8.0))]), Ok(Value::Float(OrderedFloat(3.0))));
    }

    #[test]
    fn test_log10() {
        let r = make_registry();
        assert_eq!(r.call("log10", &[Value::Float(OrderedFloat(100.0))]), Ok(Value::Float(OrderedFloat(2.0))));
    }

    #[test]
    fn test_exp() {
        let r = make_registry();
        assert_eq!(r.call("exp", &[Value::Float(OrderedFloat(0.0))]), Ok(Value::Float(OrderedFloat(1.0))));
    }

    #[test]
    fn test_exp_int() {
        let r = make_registry();
        assert_eq!(r.call("exp", &[Value::Int(0)]), Ok(Value::Float(OrderedFloat(1.0))));
    }

    #[test]
    fn test_atan2() {
        let r = make_registry();
        assert_eq!(
            r.call("atan2", &[Value::Float(OrderedFloat(1.0)), Value::Float(OrderedFloat(1.0))]),
            Ok(Value::Float(OrderedFloat(1.0_f32.atan2(1.0))))
        );
    }

    #[test]
    fn test_pi() {
        let r = make_registry();
        assert_eq!(r.call("pi", &[]), Ok(Value::Float(OrderedFloat(std::f32::consts::PI))));
    }

    #[test]
    fn test_e_constant() {
        let r = make_registry();
        assert_eq!(r.call("e", &[]), Ok(Value::Float(OrderedFloat(std::f32::consts::E))));
    }

    #[test]
    fn test_infinity() {
        let r = make_registry();
        assert_eq!(r.call("infinity", &[]), Ok(Value::Float(OrderedFloat(f32::INFINITY))));
    }

    #[test]
    fn test_nan_constant() {
        let r = make_registry();
        let result = r.call("nan", &[]);
        match result {
            Ok(Value::Float(v)) => assert!(v.into_inner().is_nan()),
            other => panic!("Expected Float(NaN), got {:?}", other),
        }
    }

    #[test]
    fn test_is_nan() {
        let r = make_registry();
        assert_eq!(r.call("is_nan", &[Value::Float(OrderedFloat(f32::NAN))]), Ok(Value::Boolean(true)));
        assert_eq!(r.call("is_nan", &[Value::Float(OrderedFloat(1.0))]), Ok(Value::Boolean(false)));
    }

    #[test]
    fn test_is_finite() {
        let r = make_registry();
        assert_eq!(r.call("is_finite", &[Value::Float(OrderedFloat(1.0))]), Ok(Value::Boolean(true)));
        assert_eq!(r.call("is_finite", &[Value::Float(OrderedFloat(f32::INFINITY))]), Ok(Value::Boolean(false)));
    }

    #[test]
    fn test_is_infinite() {
        let r = make_registry();
        assert_eq!(r.call("is_infinite", &[Value::Float(OrderedFloat(f32::INFINITY))]), Ok(Value::Boolean(true)));
        assert_eq!(r.call("is_infinite", &[Value::Float(OrderedFloat(f32::NEG_INFINITY))]), Ok(Value::Boolean(true)));
        assert_eq!(r.call("is_infinite", &[Value::Float(OrderedFloat(1.0))]), Ok(Value::Boolean(false)));
        assert_eq!(r.call("is_infinite", &[Value::Float(OrderedFloat(f32::NAN))]), Ok(Value::Boolean(false)));
    }

    #[test]
    fn test_sin() {
        let r = make_registry();
        let result = r.call("sin", &[Value::Float(OrderedFloat(0.0))]);
        assert_eq!(result, Ok(Value::Float(OrderedFloat(0.0))));
    }

    #[test]
    fn test_cos() {
        let r = make_registry();
        let result = r.call("cos", &[Value::Float(OrderedFloat(0.0))]);
        assert_eq!(result, Ok(Value::Float(OrderedFloat(1.0))));
    }

    #[test]
    fn test_tan() {
        let r = make_registry();
        let result = r.call("tan", &[Value::Float(OrderedFloat(0.0))]);
        assert_eq!(result, Ok(Value::Float(OrderedFloat(0.0))));
    }

    #[test]
    fn test_asin() {
        let r = make_registry();
        let result = r.call("asin", &[Value::Float(OrderedFloat(0.0))]);
        assert_eq!(result, Ok(Value::Float(OrderedFloat(0.0))));
    }

    #[test]
    fn test_acos() {
        let r = make_registry();
        let result = r.call("acos", &[Value::Float(OrderedFloat(1.0))]);
        assert_eq!(result, Ok(Value::Float(OrderedFloat(0.0))));
    }

    #[test]
    fn test_atan() {
        let r = make_registry();
        let result = r.call("atan", &[Value::Float(OrderedFloat(0.0))]);
        assert_eq!(result, Ok(Value::Float(OrderedFloat(0.0))));
    }

    #[test]
    fn test_cosh() {
        let r = make_registry();
        let result = r.call("cosh", &[Value::Float(OrderedFloat(0.0))]);
        assert_eq!(result, Ok(Value::Float(OrderedFloat(1.0))));
    }

    #[test]
    fn test_tanh() {
        let r = make_registry();
        let result = r.call("tanh", &[Value::Float(OrderedFloat(0.0))]);
        assert_eq!(result, Ok(Value::Float(OrderedFloat(0.0))));
    }

    #[test]
    fn test_radians() {
        let r = make_registry();
        let result = r.call("radians", &[Value::Float(OrderedFloat(180.0))]);
        assert_eq!(result, Ok(Value::Float(OrderedFloat(std::f32::consts::PI))));
    }

    #[test]
    fn test_degrees() {
        let r = make_registry();
        let result = r.call("degrees", &[Value::Float(OrderedFloat(std::f32::consts::PI))]);
        assert_eq!(result, Ok(Value::Float(OrderedFloat(180.0))));
    }

    #[test]
    fn test_rand_returns_float() {
        let r = make_registry();
        let result = r.call("rand", &[]);
        match result {
            Ok(Value::Float(v)) => {
                let f = v.into_inner();
                assert!(f >= 0.0 && f < 1.0, "rand() should return [0.0, 1.0), got {}", f);
            }
            other => panic!("Expected Float, got {:?}", other),
        }
    }

    #[test]
    fn test_random_alias() {
        let r = make_registry();
        let result = r.call("random", &[]);
        match result {
            Ok(Value::Float(v)) => {
                let f = v.into_inner();
                assert!(f >= 0.0 && f < 1.0, "random() should return [0.0, 1.0), got {}", f);
            }
            other => panic!("Expected Float, got {:?}", other),
        }
    }

    #[test]
    fn test_trig_with_int() {
        let r = make_registry();
        assert_eq!(r.call("sin", &[Value::Int(0)]), Ok(Value::Float(OrderedFloat(0.0))));
        assert_eq!(r.call("cos", &[Value::Int(0)]), Ok(Value::Float(OrderedFloat(1.0))));
    }
}
