use logq::common::types::Value;
use logq::functions::register_all;
use ordered_float::OrderedFloat;

#[test]
fn array_sort_uses_numeric_order_without_rounding_integers() {
    let registry = register_all().unwrap();
    assert_eq!(
        registry.call(
            "array_sort",
            &[Value::Array(vec![
                Value::Int(16_777_217),
                Value::Float(OrderedFloat(16_777_216.0)),
                Value::Int(16_777_215)
            ])]
        ),
        Ok(Value::Array(vec![
            Value::Int(16_777_215),
            Value::Float(OrderedFloat(16_777_216.0)),
            Value::Int(16_777_217)
        ]))
    );
}

#[test]
fn array_sort_uses_a_consistent_order_for_mixed_types() {
    let registry = register_all().unwrap();
    let expected = vec![Value::Int(2), Value::Int(10), Value::String("11".into()), Value::Null];
    for values in [
        vec![Value::Int(2), Value::Int(10), Value::String("11".into()), Value::Null],
        vec![Value::String("11".into()), Value::Int(10), Value::Null, Value::Int(2)],
    ] {
        assert_eq!(
            registry.call("array_sort", &[Value::Array(values)]),
            Ok(Value::Array(expected.clone()))
        );
    }
}

#[test]
fn public_registry_calls_reject_invalid_arity() {
    let registry = register_all().unwrap();
    for (name, args) in [
        ("abs", vec![]),
        ("abs", vec![Value::Int(1), Value::Int(2)]),
        ("substring", vec![Value::String("hello".into())]),
        ("concat_ws", vec![]),
        ("pi", vec![Value::Int(1)]),
    ] {
        assert!(registry.call(name, &args).is_err(), "{name}");
    }
}

#[test]
fn character_lengths_count_unicode_scalars() {
    let registry = register_all().unwrap();
    for name in ["char_length", "character_length"] {
        for (text, length) in [("", 0), ("hello", 5), ("臺灣🦀", 3), ("e\u{301}", 2)] {
            assert_eq!(
                registry.call(name, &[Value::String(text.into())]),
                Ok(Value::Int(length))
            );
        }
    }
}

#[test]
fn string_positions_handle_full_integer_range() {
    let registry = register_all().unwrap();
    for index in [i32::MIN, -1, 0] {
        assert_eq!(
            registry.call("substring", &[Value::String("臺灣🦀".into()), Value::Int(index)]),
            Ok(Value::String("臺灣🦀".into()))
        );
        assert_eq!(
            registry.call(
                "split_part",
                &[
                    Value::String("a/b/c".into()),
                    Value::String("/".into()),
                    Value::Int(index)
                ]
            ),
            Ok(Value::String("".into()))
        );
    }
    assert_eq!(
        registry.call("substring", &[Value::String("臺灣🦀".into()), Value::Int(i32::MAX)]),
        Ok(Value::String("".into()))
    );
    assert_eq!(
        registry.call(
            "split_part",
            &[
                Value::String("a/b/c".into()),
                Value::String("/".into()),
                Value::Int(i32::MAX)
            ]
        ),
        Ok(Value::String("".into()))
    );
}

#[test]
fn integer_arithmetic_overflow_returns_errors() {
    let registry = register_all().unwrap();
    for (name, args) in [
        ("plus", vec![Value::Int(i32::MAX), Value::Int(1)]),
        ("minus", vec![Value::Int(i32::MIN), Value::Int(1)]),
        ("times", vec![Value::Int(i32::MAX), Value::Int(2)]),
        ("divide", vec![Value::Int(i32::MIN), Value::Int(-1)]),
        ("abs", vec![Value::Int(i32::MIN)]),
    ] {
        assert!(registry.call(name, &args).is_err(), "{name}");
    }
}

#[test]
fn remainder_of_minimum_integer_by_minus_one_is_zero() {
    let registry = register_all().unwrap();
    for name in ["mod", "modulus"] {
        assert_eq!(
            registry.call(name, &[Value::Int(i32::MIN), Value::Int(-1)]),
            Ok(Value::Int(0))
        );
        assert_eq!(registry.call(name, &[Value::Int(1), Value::Int(0)]), Ok(Value::Null));
    }
}

#[test]
fn bit_shifts_validate_the_shift_width() {
    let registry = register_all().unwrap();
    for name in ["bitwise_shift_left", "bitwise_shift_right"] {
        for width in [i32::MIN, -1, 32, i32::MAX] {
            assert!(
                registry.call(name, &[Value::Int(1), Value::Int(width)]).is_err(),
                "{name}({width})"
            );
        }
    }
    assert_eq!(
        registry.call("bitwise_shift_left", &[Value::Int(1), Value::Int(31)]),
        Ok(Value::Int(i32::MIN))
    );
    assert_eq!(
        registry.call("bitwise_shift_right", &[Value::Int(-2), Value::Int(1)]),
        Ok(Value::Int(-1))
    );
}

#[test]
fn unicode_edit_distance_is_symmetric_and_counts_scalars() {
    let registry = register_all().unwrap();
    for (left, right, expected) in [
        ("", "臺灣🦀", 3),
        ("臺灣🦀", "台灣🦀", 1),
        ("kitten", "sitting", 3),
        ("e\u{301}", "é", 2),
    ] {
        for (a, b) in [(left, right), (right, left)] {
            assert_eq!(
                registry.call(
                    "levenshtein_distance",
                    &[Value::String(a.into()), Value::String(b.into())]
                ),
                Ok(Value::Int(expected))
            );
        }
    }
}
