use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use logq::bench_internals::*;
use logq::common::types as ctypes;
use logq::functions;

mod helpers;
use helpers::queries::*;
use helpers::synthetic::generate_records;

/// Tier A: End-to-end benchmarks using real AWSELB.log
fn bench_execution_tier_a(c: &mut Criterion) {
    let path = PathBuf::from("data/AWSELB.log");
    if !path.exists() {
        eprintln!("Skipping Tier A benchmarks: data/AWSELB.log not found");
        return;
    }

    let mut group = c.benchmark_group("execution_e2e");

    let queries = [
        ("E1_scan_limit", EXEC_E1),
        ("E2_groupby_count", EXEC_E2),
        ("E3_filter_orderby", EXEC_E3),
    ];

    for (name, query) in &queries {
        let data_source = ctypes::DataSource::File(
            path.clone(),
            "elb".to_string(),
            "elb".to_string(),
        );
        group.bench_function(*name, |b| {
            b.iter(|| {
                let result = logq::app::run_to_records(
                    black_box(query),
                    data_source.clone(),
                );
                let _ = black_box(result);
            });
        });
    }

    group.finish();
}

/// Tier B: Isolated operator benchmarks with synthetic data
fn bench_execution_tier_b(c: &mut Criterion) {
    let sizes: &[(usize, &str)] = &[
        (1_000, "1K"),
        (10_000, "10K"),
        (100_000, "100K"),
    ];

    let registry = Arc::new(functions::register_all().unwrap());

    // S1: MapStream -- SELECT projection (extract 2 of 5 columns)
    {
        let mut group = c.benchmark_group("execution_map");
        for &(size, label) in sizes {
            if size >= 100_000 {
                group.measurement_time(Duration::from_secs(10));
                group.sample_size(20);
            }
            let reg = registry.clone();
            group.bench_with_input(BenchmarkId::from_parameter(label), &size, |b, &sz| {
                b.iter_batched(
                    || generate_records(sz),
                    |data| {
                        let source = InMemoryStream::new(data);
                        let named_list = vec![
                            Named::Expression(
                                Expression::Variable(PathExpr::new(vec![
                                    PathSegment::AttrName("col_s1".to_string()),
                                ])),
                                Some("col_s1".to_string()),
                            ),
                            Named::Expression(
                                Expression::Variable(PathExpr::new(vec![
                                    PathSegment::AttrName("col_i".to_string()),
                                ])),
                                Some("col_i".to_string()),
                            ),
                        ];
                        let variables = ctypes::Variables::default();
                        let mut stream = MapStream::new(named_list, variables, Box::new(source), reg.clone());
                        let mut count = 0u64;
                        while let Ok(Some(_)) = stream.next() {
                            count += 1;
                        }
                        black_box(count)
                    },
                    criterion::BatchSize::SmallInput,
                );
            });
        }
        group.finish();
    }

    // S2: FilterStream -- WHERE col_i > 500 (filters ~half)
    {
        let mut group = c.benchmark_group("execution_filter");
        for &(size, label) in sizes {
            if size >= 100_000 {
                group.measurement_time(Duration::from_secs(10));
                group.sample_size(20);
            }
            let reg = registry.clone();
            group.bench_with_input(BenchmarkId::from_parameter(label), &size, |b, &sz| {
                b.iter_batched(
                    || generate_records(sz),
                    |data| {
                        let source = InMemoryStream::new(data);
                        let formula = Formula::Predicate(
                            Relation::MoreThan,
                            Box::new(Expression::Variable(PathExpr::new(vec![
                                PathSegment::AttrName("col_i".to_string()),
                            ]))),
                            Box::new(Expression::Constant(Value::Int(500))),
                        );
                        let variables = ctypes::Variables::default();
                        let mut stream = FilterStream::new(formula, variables, Box::new(source), reg.clone());
                        let mut count = 0u64;
                        while let Ok(Some(_)) = stream.next() {
                            count += 1;
                        }
                        black_box(count)
                    },
                    criterion::BatchSize::SmallInput,
                );
            });
        }
        group.finish();
    }

    // S3: LimitStream -- LIMIT 100 (should be near-constant)
    {
        let mut group = c.benchmark_group("execution_limit");
        for &(size, label) in sizes {
            group.bench_with_input(BenchmarkId::from_parameter(label), &size, |b, &sz| {
                b.iter_batched(
                    || generate_records(sz),
                    |data| {
                        let source = InMemoryStream::new(data);
                        let mut stream = LimitStream::new(100, Box::new(source));
                        let mut count = 0u64;
                        while let Ok(Some(_)) = stream.next() {
                            count += 1;
                        }
                        black_box(count)
                    },
                    criterion::BatchSize::SmallInput,
                );
            });
        }
        group.finish();
    }
}

criterion_group!(benches, bench_execution_tier_a, bench_execution_tier_b);
criterion_main!(benches);
