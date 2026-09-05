use criterion::{Criterion, Throughput, criterion_group, criterion_main};
use logq::bench_internals::*;
use std::hint::black_box;

fn load_and_replicate(path: &str, min_lines: usize) -> (Vec<String>, usize) {
    let content = std::fs::read_to_string(path).unwrap();
    let lines: Vec<String> = content
        .lines()
        .filter(|l| !l.is_empty())
        .map(|l| l.to_string())
        .collect();
    let original_count = lines.len();
    if original_count >= min_lines {
        return (lines, original_count);
    }
    let reps = min_lines.div_ceil(original_count);
    let mut replicated = Vec::with_capacity(reps * original_count);
    for _ in 0..reps {
        replicated.extend(lines.iter().cloned());
    }
    let count = replicated.len();
    (replicated, count)
}

fn bench_datasource(c: &mut Criterion) {
    let formats = [
        ("ELB", "data/AWSELB.log", "elb", 500),
        ("ALB", "data/AWSALB.log", "alb", 1000),
        ("S3", "data/S3.log", "s3", 1000),
        ("Squid", "data/Squid.log", "squid", 1000),
        ("JSONL", "data/structured.log", "jsonl", 1000),
    ];

    let mut group = c.benchmark_group("datasource");

    for (name, path, format, min_lines) in &formats {
        let (lines, line_count) = load_and_replicate(path, *min_lines);
        let concatenated = lines.join("\n") + "\n";

        group.throughput(Throughput::Elements(line_count as u64));
        group.bench_function(*name, |b| {
            b.iter(|| {
                let reader_builder = ReaderBuilder::new(format.to_string());
                let cursor = std::io::Cursor::new(concatenated.as_bytes());
                let mut reader = reader_builder.with_reader(cursor).unwrap();
                let mut count = 0u64;
                while reader.read_record().expect("datasource benchmark failed").is_some() {
                    count += 1;
                }
                assert_eq!(count, line_count as u64);
                black_box(count)
            });
        });
    }

    group.finish();
}

fn bench_json_batch_scan(c: &mut Criterion) {
    let mut group = c.benchmark_group("json_batch_scan");
    let rows = 4096u64;
    for (name, length, unique) in [
        ("short_repeated", 8, false),
        ("short_unique", 8, true),
        ("long_repeated", 256, false),
        ("long_unique", 256, true),
    ] {
        let mut input = Vec::new();
        for row in 0..rows {
            let key = if unique { row } else { row % 5 };
            let value = format!("{key:08}-{}", "payload".repeat(length / 7));
            use std::io::Write;
            writeln!(&mut input, "{{\"n\":{row},\"s\":\"{value}\"}}").unwrap();
        }
        let data: std::sync::Arc<[u8]> = input.into();
        group.throughput(Throughput::Bytes(data.len() as u64));
        for dictionary in [false, true] {
            group.bench_function(format!("{name}/dictionary_{dictionary}"), |b| {
                b.iter(|| {
                    let reader = Box::new(std::io::Cursor::new(std::sync::Arc::clone(&data)));
                    let mut scanner = json_batch_scanner(reader, vec!["n".into(), "s".into()], dictionary);
                    let mut count = 0;
                    while let Some(batch) = scanner.next_batch().expect("JSON batch benchmark failed") {
                        count += batch.len as u64;
                        black_box(batch);
                    }
                    assert_eq!(count, rows);
                    black_box(count)
                });
            });
        }
    }
    group.finish();
}

criterion_group!(benches, bench_datasource, bench_json_batch_scan);
criterion_main!(benches);
