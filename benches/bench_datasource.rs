use criterion::{black_box, criterion_group, criterion_main, Criterion, Throughput};
use logq::bench_internals::*;

fn load_and_replicate(path: &str, min_lines: usize) -> (Vec<String>, usize) {
    let content = std::fs::read_to_string(path).unwrap();
    let lines: Vec<String> = content.lines().filter(|l| !l.is_empty()).map(|l| l.to_string()).collect();
    let original_count = lines.len();
    if original_count >= min_lines {
        return (lines, original_count);
    }
    let reps = (min_lines + original_count - 1) / original_count;
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
                let mut reader = reader_builder.with_reader(cursor);
                let mut count = 0u64;
                while let Ok(Some(_record)) = reader.read_record() {
                    count += 1;
                }
                black_box(count)
            });
        });
    }

    group.finish();
}

criterion_group!(benches, bench_datasource);
criterion_main!(benches);
