use crate::common;
use crate::common::types::Value;
use crate::execution::types::ExpressionError;
use crate::functions::registry::{FunctionDef, FunctionRegistry, Arity, NullHandling, RegistryError};
use chrono::Timelike;
use chrono::Datelike;
use ordered_float::OrderedFloat;

pub fn register(registry: &mut FunctionRegistry) -> Result<(), RegistryError> {
    // date_part: extract a date component from a DateTime
    registry.register(FunctionDef {
        name: "date_part".to_string(),
        arity: Arity::Exact(2),
        null_handling: NullHandling::Propagate,
        func: Box::new(|args| match (&args[0], &args[1]) {
            (Value::String(date_part_unit_str), Value::DateTime(dt)) => {
                let date_part_unit = common::types::parse_date_part_unit(date_part_unit_str)?;

                match date_part_unit {
                    common::types::DatePartUnit::Second => Ok(Value::Float(OrderedFloat::from(dt.second() as f32))),
                    common::types::DatePartUnit::Minute => Ok(Value::Float(OrderedFloat::from(dt.minute() as f32))),
                    common::types::DatePartUnit::Hour => Ok(Value::Float(OrderedFloat::from(dt.hour() as f32))),
                    common::types::DatePartUnit::Day => Ok(Value::Float(OrderedFloat::from(dt.day() as f32))),
                    common::types::DatePartUnit::Month => Ok(Value::Float(OrderedFloat::from(dt.month() as f32))),
                    common::types::DatePartUnit::Year => Ok(Value::Float(OrderedFloat::from(dt.year() as f32))),
                }
            }
            _ => Err(ExpressionError::InvalidArguments),
        }),
    })?;

    // time_bucket: bucket a DateTime into time intervals
    registry.register(FunctionDef {
        name: "time_bucket".to_string(),
        arity: Arity::Exact(2),
        null_handling: NullHandling::Propagate,
        func: Box::new(|args| match (&args[0], &args[1]) {
            (Value::String(time_interval_str), Value::DateTime(dt)) => {
                let time_interval = common::types::parse_time_interval(time_interval_str)?;

                if time_interval.n == 0 {
                    return Err(ExpressionError::TimeIntervalZero);
                }

                match time_interval.unit {
                    common::types::TimeIntervalUnit::Second => {
                        if time_interval.n > 60 || 60 % time_interval.n != 0 {
                            return Err(ExpressionError::TimeIntervalNotSupported);
                        }

                        let mut target_opt: Option<u32> = None;
                        let step_size: usize = time_interval.n as usize;
                        //FIXME: binary search
                        for point in (0..=60u32).rev().step_by(step_size) {
                            if point <= dt.second() {
                                target_opt = Some(point);
                                break;
                            }
                        }

                        if let Some(target) = target_opt {
                            let new_dt = dt.with_second(target).and_then(|d| d.with_nanosecond(0)).unwrap();
                            Ok(Value::DateTime(new_dt))
                        } else {
                            unreachable!();
                        }
                    }
                    common::types::TimeIntervalUnit::Minute => {
                        if time_interval.n > 60 || 60 % time_interval.n != 0 {
                            return Err(ExpressionError::TimeIntervalNotSupported);
                        }

                        let mut target_opt: Option<u32> = None;
                        let step_size: usize = time_interval.n as usize;
                        //FIXME: binary search
                        for point in (0..=60u32).rev().step_by(step_size) {
                            if point <= dt.minute() {
                                target_opt = Some(point);
                                break;
                            }
                        }

                        if let Some(target) = target_opt {
                            let new_dt = dt
                                .with_minute(target)
                                .and_then(|d| d.with_second(0))
                                .and_then(|d| d.with_nanosecond(0))
                                .unwrap();
                            Ok(Value::DateTime(new_dt))
                        } else {
                            unreachable!();
                        }
                    }
                    common::types::TimeIntervalUnit::Hour => {
                        if time_interval.n > 24 || 24 % time_interval.n != 0 {
                            return Err(ExpressionError::TimeIntervalNotSupported);
                        }

                        let mut target_opt: Option<u32> = None;
                        let step_size: usize = time_interval.n as usize;
                        //FIXME: binary search
                        for point in (0..=24u32).rev().step_by(step_size) {
                            if point <= dt.hour() {
                                target_opt = Some(point);
                                break;
                            }
                        }

                        if let Some(target) = target_opt {
                            let new_dt = dt
                                .with_hour(target)
                                .and_then(|d| d.with_minute(0))
                                .and_then(|d| d.with_second(0))
                                .and_then(|d| d.with_nanosecond(0))
                                .unwrap();
                            Ok(Value::DateTime(new_dt))
                        } else {
                            unreachable!();
                        }
                    }
                    _ => Err(ExpressionError::TimeIntervalNotSupported),
                }
            }
            _ => Err(ExpressionError::InvalidArguments),
        }),
    })?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::types::Value;
    use ordered_float::OrderedFloat;

    fn make_registry() -> FunctionRegistry {
        let mut r = FunctionRegistry::new();
        register(&mut r).unwrap();
        r
    }

    fn make_datetime(s: &str) -> Value {
        Value::DateTime(chrono::DateTime::parse_from_rfc3339(s).unwrap())
    }

    #[test]
    fn test_date_part_year() {
        let r = make_registry();
        let dt = make_datetime("2015-11-07T18:45:37.000000Z");
        assert_eq!(
            r.call("date_part", &[Value::String("year".to_string()), dt]),
            Ok(Value::Float(OrderedFloat(2015.0)))
        );
    }

    #[test]
    fn test_date_part_month() {
        let r = make_registry();
        let dt = make_datetime("2015-11-07T18:45:37.000000Z");
        assert_eq!(
            r.call("date_part", &[Value::String("month".to_string()), dt]),
            Ok(Value::Float(OrderedFloat(11.0)))
        );
    }

    #[test]
    fn test_date_part_day() {
        let r = make_registry();
        let dt = make_datetime("2015-11-07T18:45:37.000000Z");
        assert_eq!(
            r.call("date_part", &[Value::String("day".to_string()), dt]),
            Ok(Value::Float(OrderedFloat(7.0)))
        );
    }

    #[test]
    fn test_date_part_hour() {
        let r = make_registry();
        let dt = make_datetime("2015-11-07T18:45:37.000000Z");
        assert_eq!(
            r.call("date_part", &[Value::String("hour".to_string()), dt]),
            Ok(Value::Float(OrderedFloat(18.0)))
        );
    }

    #[test]
    fn test_date_part_minute() {
        let r = make_registry();
        let dt = make_datetime("2015-11-07T18:45:37.000000Z");
        assert_eq!(
            r.call("date_part", &[Value::String("minute".to_string()), dt]),
            Ok(Value::Float(OrderedFloat(45.0)))
        );
    }

    #[test]
    fn test_date_part_second() {
        let r = make_registry();
        let dt = make_datetime("2015-11-07T18:45:37.000000Z");
        assert_eq!(
            r.call("date_part", &[Value::String("second".to_string()), dt]),
            Ok(Value::Float(OrderedFloat(37.0)))
        );
    }

    #[test]
    fn test_date_part_null_propagation() {
        let r = make_registry();
        assert_eq!(
            r.call("date_part", &[Value::String("year".to_string()), Value::Null]),
            Ok(Value::Null)
        );
        assert_eq!(
            r.call("date_part", &[Value::String("year".to_string()), Value::Missing]),
            Ok(Value::Missing)
        );
    }

    #[test]
    fn test_time_bucket_5_minutes() {
        let r = make_registry();
        let dt = make_datetime("2015-11-07T18:45:37.691548Z");
        let expected = make_datetime("2015-11-07T18:45:00.000000Z");
        assert_eq!(
            r.call("time_bucket", &[Value::String("5 minutes".to_string()), dt]),
            Ok(expected)
        );
    }

    #[test]
    fn test_time_bucket_15_seconds() {
        let r = make_registry();
        let dt = make_datetime("2015-11-07T18:45:37.691548Z");
        let expected = make_datetime("2015-11-07T18:45:30.000000Z");
        assert_eq!(
            r.call("time_bucket", &[Value::String("15 seconds".to_string()), dt]),
            Ok(expected)
        );
    }

    #[test]
    fn test_time_bucket_2_hours() {
        let r = make_registry();
        let dt = make_datetime("2015-11-07T18:45:37.691548Z");
        let expected = make_datetime("2015-11-07T18:00:00.000000Z");
        assert_eq!(
            r.call("time_bucket", &[Value::String("2 hours".to_string()), dt]),
            Ok(expected)
        );
    }

    #[test]
    fn test_time_bucket_zero_interval() {
        let r = make_registry();
        let dt = make_datetime("2015-11-07T18:45:37.691548Z");
        assert_eq!(
            r.call("time_bucket", &[Value::String("0 second".to_string()), dt]),
            Err(ExpressionError::TimeIntervalZero)
        );
    }

    #[test]
    fn test_time_bucket_unsupported_interval() {
        let r = make_registry();
        let dt = make_datetime("2015-11-07T18:45:37.691548Z");
        assert_eq!(
            r.call("time_bucket", &[Value::String("7 seconds".to_string()), dt]),
            Err(ExpressionError::TimeIntervalNotSupported)
        );
    }

    #[test]
    fn test_time_bucket_null_propagation() {
        let r = make_registry();
        assert_eq!(
            r.call("time_bucket", &[Value::String("5 minutes".to_string()), Value::Null]),
            Ok(Value::Null)
        );
        assert_eq!(
            r.call("time_bucket", &[Value::String("5 minutes".to_string()), Value::Missing]),
            Ok(Value::Missing)
        );
    }
}
