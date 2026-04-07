use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion};
use std::time::Duration;

use logq::bench_internals::*;
use rand::prelude::*;

mod helpers;

/// Generate records with a single Int column for sort benchmarking.
fn generate_int_records(count: usize) -> Vec<Record> {
    let mut rng = StdRng::seed_from_u64(42);
    let field_names = vec!["x".to_string()];
    (0..count).map(|_| {
        Record::new(&field_names, vec![Value::Int(rng.gen_range(0..1_000_000))])
    }).collect()
}

/// Generate records with a single String column.
fn generate_string_records(count: usize) -> Vec<Record> {
    let mut rng = StdRng::seed_from_u64(42);
    let field_names = vec!["x".to_string()];
    (0..count).map(|_| {
        let s: String = (0..32).map(|_| rng.gen_range(b'a'..=b'z') as char).collect();
        Record::new(&field_names, vec![Value::String(s)])
    }).collect()
}

/// Generate records with URL strings that share a long common prefix.
fn generate_url_records(count: usize) -> Vec<Record> {
    let mut rng = StdRng::seed_from_u64(42);
    let field_names = vec!["x".to_string()];
    (0..count).map(|_| {
        let suffix: String = (0..16).map(|_| rng.gen_range(b'a'..=b'z') as char).collect();
        let url = format!("https://example.com/path/{}", suffix);
        Record::new(&field_names, vec![Value::String(url)])
    }).collect()
}

/// Generate records with a single Int column where 20% of values are Null.
fn generate_int_with_nulls_records(count: usize) -> Vec<Record> {
    let mut rng = StdRng::seed_from_u64(42);
    let field_names = vec!["x".to_string()];
    (0..count).map(|_| {
        let val = if rng.gen_bool(0.2) {
            Value::Null
        } else {
            Value::Int(rng.gen_range(0..1_000_000))
        };
        Record::new(&field_names, vec![val])
    }).collect()
}

/// Generate records with multiple sort keys.
fn generate_multi_key_records(count: usize) -> Vec<Record> {
    let mut rng = StdRng::seed_from_u64(42);
    let field_names = vec!["a".to_string(), "b".to_string(), "c".to_string()];
    (0..count).map(|_| {
        Record::new(&field_names, vec![
            Value::Int(rng.gen_range(0..100)),
            Value::String(format!("str_{}", rng.gen_range(0..1000))),
            Value::Int(rng.gen_range(0..10000)),
        ])
    }).collect()
}

fn path(name: &str) -> PathExpr {
    PathExpr::new(vec![PathSegment::AttrName(name.to_string())])
}

fn bench_sort_int(c: &mut Criterion) {
    let sizes: &[(usize, &str)] = &[
        (100, "100"),
        (1_000, "1K"),
        (10_000, "10K"),
        (100_000, "100K"),
    ];

    let mut group = c.benchmark_group("sort_int");
    let keys = vec![path("x")];
    let orderings = vec![Ordering::Asc];

    for &(size, label) in sizes {
        if size >= 100_000 {
            group.measurement_time(Duration::from_secs(10));
            group.sample_size(20);
        }

        group.bench_with_input(BenchmarkId::new("prefix", label), &size, |b, &sz| {
            b.iter_batched(
                || generate_int_records(sz),
                |records| {
                    let encoder = PrefixSortEncoder { threshold: 0, ..Default::default() };
                    black_box(encoder.sort(records, &keys, &orderings))
                },
                criterion::BatchSize::SmallInput,
            );
        });

        group.bench_with_input(BenchmarkId::new("direct", label), &size, |b, &sz| {
            b.iter_batched(
                || generate_int_records(sz),
                |records| {
                    let encoder = PrefixSortEncoder { threshold: usize::MAX, ..Default::default() };
                    black_box(encoder.sort(records, &keys, &orderings))
                },
                criterion::BatchSize::SmallInput,
            );
        });
    }
    group.finish();
}

fn bench_sort_string(c: &mut Criterion) {
    let mut group = c.benchmark_group("sort_string");
    let keys = vec![path("x")];
    let orderings = vec![Ordering::Asc];
    let size = 10_000;

    group.bench_function("prefix", |b| {
        b.iter_batched(
            || generate_string_records(size),
            |records| {
                let encoder = PrefixSortEncoder { threshold: 0, ..Default::default() };
                black_box(encoder.sort(records, &keys, &orderings))
            },
            criterion::BatchSize::SmallInput,
        );
    });

    group.bench_function("direct", |b| {
        b.iter_batched(
            || generate_string_records(size),
            |records| {
                let encoder = PrefixSortEncoder { threshold: usize::MAX, ..Default::default() };
                black_box(encoder.sort(records, &keys, &orderings))
            },
            criterion::BatchSize::SmallInput,
        );
    });

    group.finish();
}

fn bench_sort_string_urls(c: &mut Criterion) {
    let mut group = c.benchmark_group("sort_string_urls");
    let keys = vec![path("x")];
    let orderings = vec![Ordering::Asc];
    let size = 10_000;

    group.bench_function("prefix", |b| {
        b.iter_batched(
            || generate_url_records(size),
            |records| {
                let encoder = PrefixSortEncoder { threshold: 0, ..Default::default() };
                black_box(encoder.sort(records, &keys, &orderings))
            },
            criterion::BatchSize::SmallInput,
        );
    });

    group.bench_function("direct", |b| {
        b.iter_batched(
            || generate_url_records(size),
            |records| {
                let encoder = PrefixSortEncoder { threshold: usize::MAX, ..Default::default() };
                black_box(encoder.sort(records, &keys, &orderings))
            },
            criterion::BatchSize::SmallInput,
        );
    });

    group.finish();
}

fn bench_sort_with_nulls(c: &mut Criterion) {
    let mut group = c.benchmark_group("sort_with_nulls");
    let keys = vec![path("x")];
    let orderings = vec![Ordering::Asc];
    let size = 10_000;

    group.bench_function("prefix", |b| {
        b.iter_batched(
            || generate_int_with_nulls_records(size),
            |records| {
                let encoder = PrefixSortEncoder { threshold: 0, ..Default::default() };
                black_box(encoder.sort(records, &keys, &orderings))
            },
            criterion::BatchSize::SmallInput,
        );
    });

    group.bench_function("direct", |b| {
        b.iter_batched(
            || generate_int_with_nulls_records(size),
            |records| {
                let encoder = PrefixSortEncoder { threshold: usize::MAX, ..Default::default() };
                black_box(encoder.sort(records, &keys, &orderings))
            },
            criterion::BatchSize::SmallInput,
        );
    });

    group.finish();
}

fn bench_sort_multi_key(c: &mut Criterion) {
    let mut group = c.benchmark_group("sort_multi_key");
    let keys = vec![path("a"), path("b"), path("c")];
    let orderings = vec![Ordering::Asc, Ordering::Desc, Ordering::Asc];
    let size = 10_000;

    group.bench_function("prefix", |b| {
        b.iter_batched(
            || generate_multi_key_records(size),
            |records| {
                let encoder = PrefixSortEncoder { threshold: 0, ..Default::default() };
                black_box(encoder.sort(records, &keys, &orderings))
            },
            criterion::BatchSize::SmallInput,
        );
    });

    group.bench_function("direct", |b| {
        b.iter_batched(
            || generate_multi_key_records(size),
            |records| {
                let encoder = PrefixSortEncoder { threshold: usize::MAX, ..Default::default() };
                black_box(encoder.sort(records, &keys, &orderings))
            },
            criterion::BatchSize::SmallInput,
        );
    });

    group.finish();
}

criterion_group!(benches, bench_sort_int, bench_sort_string, bench_sort_string_urls, bench_sort_with_nulls, bench_sort_multi_key);
criterion_main!(benches);
