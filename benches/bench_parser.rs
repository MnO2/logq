use criterion::{black_box, criterion_group, criterion_main, Criterion};

mod helpers;
use helpers::queries::*;

fn bench_parser(c: &mut Criterion) {
    let queries = [
        ("L1_trivial", PARSE_L1),
        ("L2_where", PARSE_L2),
        ("L3_group_order_limit", PARSE_L3),
        ("L4_having_like", PARSE_L4),
        ("L5_casewhen_join", PARSE_L5),
        ("L6_in_union", PARSE_L6),
    ];

    let mut group = c.benchmark_group("parser");
    for (name, query) in &queries {
        group.bench_function(*name, |b| {
            b.iter(|| {
                let ok = logq::bench_internals::parse_query(black_box(query));
                assert!(black_box(ok));
            });
        });
    }
    group.finish();
}

criterion_group!(benches, bench_parser);
criterion_main!(benches);
