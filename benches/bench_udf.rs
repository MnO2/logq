use criterion::{black_box, criterion_group, criterion_main, Criterion};
use logq::functions::{self, FunctionRegistry};
use logq::common::types::Value;
use ordered_float::OrderedFloat;
use linked_hash_map::LinkedHashMap;

fn build_registry() -> FunctionRegistry {
    functions::register_all().unwrap()
}

fn bench_udfs(c: &mut Criterion) {
    let registry = build_registry();

    let mut group = c.benchmark_group("udf");

    // U1: upper("hello world") -- String
    let args_upper = vec![Value::String("hello world".to_string())];
    group.bench_function("upper", |b| {
        b.iter(|| {
            let _ = black_box(registry.call("upper", black_box(&args_upper)));
        });
    });

    // U2: round(3.14159, 2) -- Arithmetic
    let args_round = vec![
        Value::Float(OrderedFloat::from(3.14159f32)),
        Value::Int(2),
    ];
    group.bench_function("round", |b| {
        b.iter(|| {
            let _ = black_box(registry.call("round", black_box(&args_round)));
        });
    });

    // U3: date_part("month", <fixed DateTime>) -- DateTime
    let fixed_dt = chrono::DateTime::parse_from_rfc3339("2024-06-15T10:30:00+00:00").unwrap();
    let args_datepart = vec![
        Value::String("month".to_string()),
        Value::DateTime(Box::new(fixed_dt)),
    ];
    group.bench_function("date_part", |b| {
        b.iter(|| {
            let _ = black_box(registry.call("date_part", black_box(&args_datepart)));
        });
    });

    // U4: array_contains([1,2,3,4,5], 3) -- Array
    let args_array = vec![
        Value::Array(vec![
            Value::Int(1), Value::Int(2), Value::Int(3),
            Value::Int(4), Value::Int(5),
        ]),
        Value::Int(3),
    ];
    group.bench_function("array_contains", |b| {
        b.iter(|| {
            let _ = black_box(registry.call("array_contains", black_box(&args_array)));
        });
    });

    // U5: map_keys({"a":1, "b":2}) -- Map
    let mut map = LinkedHashMap::new();
    map.insert("a".to_string(), Value::Int(1));
    map.insert("b".to_string(), Value::Int(2));
    let args_map = vec![Value::Object(Box::new(map))];
    group.bench_function("map_keys", |b| {
        b.iter(|| {
            let _ = black_box(registry.call("map_keys", black_box(&args_map)));
        });
    });

    // U6: regexp_like("foo123", "\d+") -- Regex (steady-state cached)
    let args_regex = vec![
        Value::String("foo123".to_string()),
        Value::String(r"\d+".to_string()),
    ];
    // Warm the cache
    let _ = registry.call("regexp_like", &args_regex);
    group.bench_function("regexp_like", |b| {
        b.iter(|| {
            let _ = black_box(registry.call("regexp_like", black_box(&args_regex)));
        });
    });

    group.finish();
}

criterion_group!(benches, bench_udfs);
criterion_main!(benches);
