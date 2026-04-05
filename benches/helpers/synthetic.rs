use logq::bench_internals::*;
use ordered_float::OrderedFloat;
use rand::prelude::*;
use std::collections::VecDeque;

/// Generate synthetic records with 5 columns:
/// col_s1 (String), col_s2 (String), col_i (Int), col_f (Float), col_b (Boolean)
pub fn generate_records(count: usize) -> VecDeque<Record> {
    let mut rng = StdRng::seed_from_u64(42);
    let field_names = vec![
        "col_s1".to_string(),
        "col_s2".to_string(),
        "col_i".to_string(),
        "col_f".to_string(),
        "col_b".to_string(),
    ];
    let mut records = VecDeque::with_capacity(count);
    for _ in 0..count {
        let data = vec![
            Value::String(format!("str_{}", rng.gen_range(0..100))),
            Value::String(format!("cat_{}", rng.gen_range(0..10))),
            Value::Int(rng.gen_range(0..1000)),
            Value::Float(OrderedFloat::from(rng.gen::<f32>() * 1000.0)),
            Value::Boolean(rng.gen_bool(0.5)),
        ];
        records.push_back(Record::new(&field_names, data));
    }
    records
}
