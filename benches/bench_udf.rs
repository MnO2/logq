use criterion::{Criterion, criterion_group, criterion_main};
use linked_hash_map::LinkedHashMap;
use logq::common::types::Value;
use logq::functions::{self, FunctionRegistry};
use ordered_float::OrderedFloat;
use std::hint::black_box;

fn build_registry() -> FunctionRegistry {
    functions::register_all().unwrap()
}

fn bench_udfs(c: &mut Criterion) {
    let registry = build_registry();

    let mut group = c.benchmark_group("udf");

    // U1: upper("hello world") -- String
    let args_upper = vec![Value::String("hello world".into())];
    group.bench_function("upper", |b| {
        b.iter(|| {
            let _ = black_box(
                registry
                    .call("upper", black_box(&args_upper))
                    .expect("upper benchmark failed"),
            );
        });
    });

    // U2: round(3.14159, 2) -- Arithmetic
    let args_round = vec![Value::Float(OrderedFloat::from(std::f32::consts::PI)), Value::Int(2)];
    group.bench_function("round", |b| {
        b.iter(|| {
            let _ = black_box(
                registry
                    .call("round", black_box(&args_round))
                    .expect("round benchmark failed"),
            );
        });
    });

    // U3: date_part("month", <fixed DateTime>) -- DateTime
    let fixed_dt = chrono::DateTime::parse_from_rfc3339("2024-06-15T10:30:00+00:00").unwrap();
    let args_datepart = vec![Value::String("month".into()), Value::DateTime(fixed_dt)];
    group.bench_function("date_part", |b| {
        b.iter(|| {
            let _ = black_box(
                registry
                    .call("date_part", black_box(&args_datepart))
                    .expect("date_part benchmark failed"),
            );
        });
    });

    // U4: array_contains([1,2,3,4,5], 3) -- Array
    let args_array = vec![
        Value::Array(vec![
            Value::Int(1),
            Value::Int(2),
            Value::Int(3),
            Value::Int(4),
            Value::Int(5),
        ]),
        Value::Int(3),
    ];
    group.bench_function("array_contains", |b| {
        b.iter(|| {
            let _ = black_box(
                registry
                    .call("array_contains", black_box(&args_array))
                    .expect("array_contains benchmark failed"),
            );
        });
    });

    // U5: map_keys({"a":1, "b":2}) -- Map
    let mut map = LinkedHashMap::new();
    map.insert("a".to_string(), Value::Int(1));
    map.insert("b".to_string(), Value::Int(2));
    let args_map = vec![Value::Object(Box::new(map))];
    group.bench_function("map_keys", |b| {
        b.iter(|| {
            let _ = black_box(
                registry
                    .call("map_keys", black_box(&args_map))
                    .expect("map_keys benchmark failed"),
            );
        });
    });

    // U6: regexp_like("foo123", "\d+") -- Regex (steady-state cached)
    let args_regex = vec![Value::String("foo123".into()), Value::String(r"\d+".into())];
    // Warm the cache
    registry
        .call("regexp_like", &args_regex)
        .expect("regex cache warmup failed");
    group.bench_function("regexp_like", |b| {
        b.iter(|| {
            let _ = black_box(
                registry
                    .call("regexp_like", black_box(&args_regex))
                    .expect("regexp_like benchmark failed"),
            );
        });
    });

    group.finish();
}

criterion_group!(benches, bench_udfs);
criterion_main!(benches);
