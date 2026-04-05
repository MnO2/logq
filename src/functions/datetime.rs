use crate::common;
use crate::common::types::Value;
use crate::execution::types::ExpressionError;
use crate::functions::registry::{FunctionDef, FunctionRegistry, Arity, NullHandling, RegistryError};
use chrono::Timelike;
use chrono::Datelike;
use chrono::Duration;
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

    // year: extract year component from DateTime
    registry.register(FunctionDef {
        name: "year".to_string(),
        arity: Arity::Exact(1),
        null_handling: NullHandling::Propagate,
        func: Box::new(|args| match &args[0] {
            Value::DateTime(dt) => Ok(Value::Int(dt.year() as i32)),
            _ => Err(ExpressionError::InvalidArguments),
        }),
    })?;

    // month: extract month (1-12)
    registry.register(FunctionDef {
        name: "month".to_string(),
        arity: Arity::Exact(1),
        null_handling: NullHandling::Propagate,
        func: Box::new(|args| match &args[0] {
            Value::DateTime(dt) => Ok(Value::Int(dt.month() as i32)),
            _ => Err(ExpressionError::InvalidArguments),
        }),
    })?;

    // day: extract day of month (1-31)
    registry.register(FunctionDef {
        name: "day".to_string(),
        arity: Arity::Exact(1),
        null_handling: NullHandling::Propagate,
        func: Box::new(|args| match &args[0] {
            Value::DateTime(dt) => Ok(Value::Int(dt.day() as i32)),
            _ => Err(ExpressionError::InvalidArguments),
        }),
    })?;

    // hour: extract hour (0-23)
    registry.register(FunctionDef {
        name: "hour".to_string(),
        arity: Arity::Exact(1),
        null_handling: NullHandling::Propagate,
        func: Box::new(|args| match &args[0] {
            Value::DateTime(dt) => Ok(Value::Int(dt.hour() as i32)),
            _ => Err(ExpressionError::InvalidArguments),
        }),
    })?;

    // minute: extract minute (0-59)
    registry.register(FunctionDef {
        name: "minute".to_string(),
        arity: Arity::Exact(1),
        null_handling: NullHandling::Propagate,
        func: Box::new(|args| match &args[0] {
            Value::DateTime(dt) => Ok(Value::Int(dt.minute() as i32)),
            _ => Err(ExpressionError::InvalidArguments),
        }),
    })?;

    // second: extract second (0-59)
    registry.register(FunctionDef {
        name: "second".to_string(),
        arity: Arity::Exact(1),
        null_handling: NullHandling::Propagate,
        func: Box::new(|args| match &args[0] {
            Value::DateTime(dt) => Ok(Value::Int(dt.second() as i32)),
            _ => Err(ExpressionError::InvalidArguments),
        }),
    })?;

    // millisecond: extract millisecond component
    registry.register(FunctionDef {
        name: "millisecond".to_string(),
        arity: Arity::Exact(1),
        null_handling: NullHandling::Propagate,
        func: Box::new(|args| match &args[0] {
            Value::DateTime(dt) => Ok(Value::Int(dt.timestamp_subsec_millis() as i32)),
            _ => Err(ExpressionError::InvalidArguments),
        }),
    })?;

    // quarter: extract quarter (1-4)
    registry.register(FunctionDef {
        name: "quarter".to_string(),
        arity: Arity::Exact(1),
        null_handling: NullHandling::Propagate,
        func: Box::new(|args| match &args[0] {
            Value::DateTime(dt) => Ok(Value::Int(((dt.month() - 1) / 3 + 1) as i32)),
            _ => Err(ExpressionError::InvalidArguments),
        }),
    })?;

    // week: extract ISO week number
    registry.register(FunctionDef {
        name: "week".to_string(),
        arity: Arity::Exact(1),
        null_handling: NullHandling::Propagate,
        func: Box::new(|args| match &args[0] {
            Value::DateTime(dt) => Ok(Value::Int(dt.iso_week().week() as i32)),
            _ => Err(ExpressionError::InvalidArguments),
        }),
    })?;

    // day_of_week: day of week (1=Monday, 7=Sunday)
    registry.register(FunctionDef {
        name: "day_of_week".to_string(),
        arity: Arity::Exact(1),
        null_handling: NullHandling::Propagate,
        func: Box::new(|args| match &args[0] {
            Value::DateTime(dt) => Ok(Value::Int(dt.weekday().num_days_from_monday() as i32 + 1)),
            _ => Err(ExpressionError::InvalidArguments),
        }),
    })?;

    // day_of_year: day of year (1-366)
    registry.register(FunctionDef {
        name: "day_of_year".to_string(),
        arity: Arity::Exact(1),
        null_handling: NullHandling::Propagate,
        func: Box::new(|args| match &args[0] {
            Value::DateTime(dt) => Ok(Value::Int(dt.ordinal() as i32)),
            _ => Err(ExpressionError::InvalidArguments),
        }),
    })?;

    // date_add: date_add(unit, amount, timestamp) -> DateTime
    registry.register(FunctionDef {
        name: "date_add".to_string(),
        arity: Arity::Exact(3),
        null_handling: NullHandling::Propagate,
        func: Box::new(|args| match (&args[0], &args[1], &args[2]) {
            (Value::String(unit), Value::Int(amount), Value::DateTime(dt)) => {
                let duration = match unit.as_str() {
                    "second" => Duration::seconds(*amount as i64),
                    "minute" => Duration::minutes(*amount as i64),
                    "hour" => Duration::hours(*amount as i64),
                    "day" => Duration::days(*amount as i64),
                    _ => return Err(ExpressionError::InvalidArguments),
                };
                let new_dt = *dt + duration;
                Ok(Value::DateTime(new_dt))
            }
            _ => Err(ExpressionError::InvalidArguments),
        }),
    })?;

    // date_diff: date_diff(unit, start, end) -> Int
    registry.register(FunctionDef {
        name: "date_diff".to_string(),
        arity: Arity::Exact(3),
        null_handling: NullHandling::Propagate,
        func: Box::new(|args| match (&args[0], &args[1], &args[2]) {
            (Value::String(unit), Value::DateTime(start), Value::DateTime(end)) => {
                let diff = *end - *start;
                let result = match unit.as_str() {
                    "second" => diff.num_seconds() as i32,
                    "minute" => diff.num_minutes() as i32,
                    "hour" => diff.num_hours() as i32,
                    "day" => diff.num_days() as i32,
                    _ => return Err(ExpressionError::InvalidArguments),
                };
                Ok(Value::Int(result))
            }
            _ => Err(ExpressionError::InvalidArguments),
        }),
    })?;

    // date_trunc: date_trunc(unit, timestamp) -> DateTime
    registry.register(FunctionDef {
        name: "date_trunc".to_string(),
        arity: Arity::Exact(2),
        null_handling: NullHandling::Propagate,
        func: Box::new(|args| match (&args[0], &args[1]) {
            (Value::String(unit), Value::DateTime(dt)) => {
                let new_dt = match unit.as_str() {
                    "second" => dt.with_nanosecond(0).unwrap(),
                    "minute" => dt.with_nanosecond(0).and_then(|d| d.with_second(0)).unwrap(),
                    "hour" => dt.with_nanosecond(0)
                        .and_then(|d| d.with_second(0))
                        .and_then(|d| d.with_minute(0))
                        .unwrap(),
                    "day" => dt.with_nanosecond(0)
                        .and_then(|d| d.with_second(0))
                        .and_then(|d| d.with_minute(0))
                        .and_then(|d| d.with_hour(0))
                        .unwrap(),
                    _ => return Err(ExpressionError::InvalidArguments),
                };
                Ok(Value::DateTime(new_dt))
            }
            _ => Err(ExpressionError::InvalidArguments),
        }),
    })?;

    // from_unixtime: from_unixtime(epoch) -> DateTime
    registry.register(FunctionDef {
        name: "from_unixtime".to_string(),
        arity: Arity::Exact(1),
        null_handling: NullHandling::Propagate,
        func: Box::new(|args| match &args[0] {
            Value::Int(epoch) => {
                let naive = chrono::NaiveDateTime::from_timestamp_opt(*epoch as i64, 0)
                    .ok_or(ExpressionError::InvalidArguments)?;
                let fixed = chrono::DateTime::<chrono::FixedOffset>::from_utc(
                    naive,
                    chrono::FixedOffset::east(0),
                );
                Ok(Value::DateTime(fixed))
            }
            _ => Err(ExpressionError::InvalidArguments),
        }),
    })?;

    // to_unixtime: to_unixtime(timestamp) -> Int
    registry.register(FunctionDef {
        name: "to_unixtime".to_string(),
        arity: Arity::Exact(1),
        null_handling: NullHandling::Propagate,
        func: Box::new(|args| match &args[0] {
            Value::DateTime(dt) => Ok(Value::Int(dt.timestamp() as i32)),
            _ => Err(ExpressionError::InvalidArguments),
        }),
    })?;

    // now: returns current DateTime
    registry.register(FunctionDef {
        name: "now".to_string(),
        arity: Arity::Exact(0),
        null_handling: NullHandling::Propagate,
        func: Box::new(|_args| {
            let utc_now = chrono::Utc::now();
            let fixed = utc_now.with_timezone(&chrono::FixedOffset::east(0));
            Ok(Value::DateTime(fixed))
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

    #[test]
    fn test_year() {
        let r = make_registry();
        let dt = make_datetime("2023-06-15T10:30:45Z");
        assert_eq!(r.call("year", &[dt]), Ok(Value::Int(2023)));
    }

    #[test]
    fn test_month() {
        let r = make_registry();
        let dt = make_datetime("2023-06-15T10:30:45Z");
        assert_eq!(r.call("month", &[dt]), Ok(Value::Int(6)));
    }

    #[test]
    fn test_day() {
        let r = make_registry();
        let dt = make_datetime("2023-06-15T10:30:45Z");
        assert_eq!(r.call("day", &[dt]), Ok(Value::Int(15)));
    }

    #[test]
    fn test_hour() {
        let r = make_registry();
        let dt = make_datetime("2023-06-15T10:30:45Z");
        assert_eq!(r.call("hour", &[dt]), Ok(Value::Int(10)));
    }

    #[test]
    fn test_minute() {
        let r = make_registry();
        let dt = make_datetime("2023-06-15T10:30:45Z");
        assert_eq!(r.call("minute", &[dt]), Ok(Value::Int(30)));
    }

    #[test]
    fn test_second() {
        let r = make_registry();
        let dt = make_datetime("2023-06-15T10:30:45Z");
        assert_eq!(r.call("second", &[dt]), Ok(Value::Int(45)));
    }

    #[test]
    fn test_millisecond() {
        let r = make_registry();
        let dt = make_datetime("2023-06-15T10:30:45.123Z");
        assert_eq!(r.call("millisecond", &[dt]), Ok(Value::Int(123)));
    }

    #[test]
    fn test_quarter() {
        let r = make_registry();
        let dt = make_datetime("2023-06-15T10:30:45Z");
        assert_eq!(r.call("quarter", &[dt]), Ok(Value::Int(2)));
    }

    #[test]
    fn test_week() {
        let r = make_registry();
        let dt = make_datetime("2023-06-15T10:30:45Z");
        let result = r.call("week", &[dt]);
        assert!(result.is_ok());
        if let Ok(Value::Int(w)) = result {
            assert!(w >= 1 && w <= 53);
        }
    }

    #[test]
    fn test_day_of_week() {
        let r = make_registry();
        let dt = make_datetime("2023-06-15T10:30:45Z"); // Thursday
        assert_eq!(r.call("day_of_week", &[dt]), Ok(Value::Int(4))); // Thu = 4
    }

    #[test]
    fn test_day_of_year() {
        let r = make_registry();
        let dt = make_datetime("2023-06-15T10:30:45Z");
        let result = r.call("day_of_year", &[dt]);
        assert!(result.is_ok());
        if let Ok(Value::Int(d)) = result {
            assert!(d >= 1 && d <= 366);
        }
    }

    #[test]
    fn test_date_trunc_hour() {
        let r = make_registry();
        let dt = make_datetime("2023-06-15T10:30:45Z");
        let expected = make_datetime("2023-06-15T10:00:00Z");
        assert_eq!(r.call("date_trunc", &[Value::String("hour".into()), dt]), Ok(expected));
    }

    #[test]
    fn test_date_trunc_minute() {
        let r = make_registry();
        let dt = make_datetime("2023-06-15T10:30:45Z");
        let expected = make_datetime("2023-06-15T10:30:00Z");
        assert_eq!(r.call("date_trunc", &[Value::String("minute".into()), dt]), Ok(expected));
    }

    #[test]
    fn test_date_trunc_day() {
        let r = make_registry();
        let dt = make_datetime("2023-06-15T10:30:45Z");
        let expected = make_datetime("2023-06-15T00:00:00Z");
        assert_eq!(r.call("date_trunc", &[Value::String("day".into()), dt]), Ok(expected));
    }

    #[test]
    fn test_date_trunc_second() {
        let r = make_registry();
        let dt = make_datetime("2023-06-15T10:30:45.123Z");
        let expected = make_datetime("2023-06-15T10:30:45Z");
        assert_eq!(r.call("date_trunc", &[Value::String("second".into()), dt]), Ok(expected));
    }

    #[test]
    fn test_date_add_day() {
        let r = make_registry();
        let dt = make_datetime("2023-06-15T10:30:45Z");
        let expected = make_datetime("2023-06-17T10:30:45Z");
        assert_eq!(r.call("date_add", &[Value::String("day".into()), Value::Int(2), dt]), Ok(expected));
    }

    #[test]
    fn test_date_add_hour() {
        let r = make_registry();
        let dt = make_datetime("2023-06-15T10:30:45Z");
        let expected = make_datetime("2023-06-15T13:30:45Z");
        assert_eq!(r.call("date_add", &[Value::String("hour".into()), Value::Int(3), dt]), Ok(expected));
    }

    #[test]
    fn test_date_add_minute() {
        let r = make_registry();
        let dt = make_datetime("2023-06-15T10:30:45Z");
        let expected = make_datetime("2023-06-15T10:45:45Z");
        assert_eq!(r.call("date_add", &[Value::String("minute".into()), Value::Int(15), dt]), Ok(expected));
    }

    #[test]
    fn test_date_add_second() {
        let r = make_registry();
        let dt = make_datetime("2023-06-15T10:30:45Z");
        let expected = make_datetime("2023-06-15T10:31:15Z");
        assert_eq!(r.call("date_add", &[Value::String("second".into()), Value::Int(30), dt]), Ok(expected));
    }

    #[test]
    fn test_date_diff_day() {
        let r = make_registry();
        let start = make_datetime("2023-06-15T10:30:45Z");
        let end = make_datetime("2023-06-20T10:30:45Z");
        assert_eq!(r.call("date_diff", &[Value::String("day".into()), start, end]), Ok(Value::Int(5)));
    }

    #[test]
    fn test_date_diff_hour() {
        let r = make_registry();
        let start = make_datetime("2023-06-15T10:30:45Z");
        let end = make_datetime("2023-06-15T13:30:45Z");
        assert_eq!(r.call("date_diff", &[Value::String("hour".into()), start, end]), Ok(Value::Int(3)));
    }

    #[test]
    fn test_date_diff_minute() {
        let r = make_registry();
        let start = make_datetime("2023-06-15T10:30:45Z");
        let end = make_datetime("2023-06-15T10:45:45Z");
        assert_eq!(r.call("date_diff", &[Value::String("minute".into()), start, end]), Ok(Value::Int(15)));
    }

    #[test]
    fn test_date_diff_second() {
        let r = make_registry();
        let start = make_datetime("2023-06-15T10:30:45Z");
        let end = make_datetime("2023-06-15T10:31:15Z");
        assert_eq!(r.call("date_diff", &[Value::String("second".into()), start, end]), Ok(Value::Int(30)));
    }

    #[test]
    fn test_to_unixtime() {
        let r = make_registry();
        let dt = make_datetime("2023-06-15T10:30:45Z");
        let result = r.call("to_unixtime", &[dt]);
        assert!(result.is_ok());
        if let Ok(Value::Int(ts)) = result {
            assert!(ts > 0);
        }
    }

    #[test]
    fn test_from_unixtime() {
        let r = make_registry();
        // First get the unix timestamp for a known datetime
        let dt = make_datetime("2023-06-15T10:30:45Z");
        let ts = if let Ok(Value::Int(ts)) = r.call("to_unixtime", &[dt.clone()]) {
            ts
        } else {
            panic!("to_unixtime failed");
        };
        // Convert back
        let result = r.call("from_unixtime", &[Value::Int(ts)]);
        assert!(result.is_ok());
        if let Ok(Value::DateTime(result_dt)) = result {
            // Should match the original (minus sub-second precision)
            assert_eq!(result_dt.year(), 2023);
            assert_eq!(result_dt.month(), 6);
            assert_eq!(result_dt.day(), 15);
            assert_eq!(result_dt.hour(), 10);
            assert_eq!(result_dt.minute(), 30);
            assert_eq!(result_dt.second(), 45);
        } else {
            panic!("Expected DateTime");
        }
    }

    #[test]
    fn test_now() {
        let r = make_registry();
        let result = r.call("now", &[]);
        assert!(result.is_ok());
        // Just verify it returns a DateTime
        if let Ok(Value::DateTime(_)) = result {
            // ok
        } else {
            panic!("Expected DateTime");
        }
    }
}
