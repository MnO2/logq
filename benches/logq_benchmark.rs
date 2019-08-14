#[macro_use]
extern crate criterion;

use criterion::Criterion;

fn bench_parse_query() {
    let _ = 1 + 1;
}

fn criterion_benchmark(c: &mut Criterion) {
    c.bench_function("parse query", |b| b.iter(bench_parse_query));
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
