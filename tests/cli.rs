use std::process::Command;

#[path = "../benches/helpers/queries.rs"]
mod benchmark_queries;

fn output(args: &[&str]) -> String {
    let output = Command::new(env!("CARGO_BIN_EXE_logq")).args(args).output().unwrap();
    assert!(output.status.success());
    String::from_utf8(output.stdout).unwrap()
}

#[test]
fn version_comes_from_the_package_manifest() {
    let stdout = output(&["--version"]);
    assert!(stdout.contains(env!("CARGO_PKG_VERSION")));
}

#[test]
fn query_and_explain_fail_with_stderr_and_a_nonzero_exit_code() {
    for command in ["query", "explain"] {
        for (table, query) in [
            ("it:clf=data/structured.log", "selec * from it"),
            ("it:jsonl=data/structured.log", "select * from unknown"),
            ("invalid", "select * from it"),
        ] {
            let result = Command::new(env!("CARGO_BIN_EXE_logq"))
                .args([command, "--table", table, query])
                .output()
                .unwrap();
            assert!(!result.status.success(), "{command}: {table}: {query}");
            assert!(result.stdout.is_empty(), "diagnostics polluted stdout: {result:?}");
            assert!(!result.stderr.is_empty(), "missing diagnostic: {result:?}");
        }
    }
}

#[test]
fn execution_sort_benchmark_executes_the_fixture_rows() {
    let stdout = output(&[
        "query",
        "--output",
        "csv",
        "--table",
        "elb:elb=data/AWSELB.log",
        benchmark_queries::EXEC_E3,
    ]);
    assert_eq!(stdout.lines().count(), 538);
    assert!(stdout.lines().all(|line| line.ends_with(",200")));
    assert!(stdout.lines().zip(stdout.lines().skip(1)).all(|(a, b)| a <= b));
}

#[cfg(feature = "bench-internals")]
#[test]
fn parser_benchmarks_require_complete_successful_queries() {
    for query in [
        benchmark_queries::PARSE_L1,
        benchmark_queries::PARSE_L2,
        benchmark_queries::PARSE_L3,
        benchmark_queries::PARSE_L4,
        benchmark_queries::PARSE_L5,
        benchmark_queries::PARSE_L6,
    ] {
        assert!(logq::bench_internals::parse_query(query), "invalid benchmark: {query}");
    }
    assert!(!logq::bench_internals::parse_query("SELECT a FROM t WHERE"));
    assert!(!logq::bench_internals::parse_query("SELECT a FROM t ORDER BY a"));
}

#[test]
fn query_help_preserves_public_options() {
    let stdout = output(&["query", "--help"]);
    assert!(stdout.contains("--output"));
    assert!(stdout.contains("--table"));
    assert!(stdout.contains("--threads"));
    assert!(stdout.contains("--max-memory"));
    assert!(stdout.contains("--format-file"));
}

#[test]
fn explain_help_preserves_table_option() {
    let stdout = output(&["explain", "--help"]);
    assert!(stdout.contains("--table"));
    assert!(stdout.contains("--format-file"));
    assert!(stdout.contains("--threads"));
    assert!(stdout.contains("--max-memory"));
}

#[test]
fn explain_validates_the_same_memory_option_as_query() {
    for command in ["query", "explain"] {
        let result = Command::new(env!("CARGO_BIN_EXE_logq"))
            .args([
                command,
                "--table",
                "it:jsonl=data/structured.log",
                "--max-memory",
                "invalid",
                "select count(*) from it",
            ])
            .output()
            .unwrap();
        assert!(!result.status.success());
        assert!(result.stdout.is_empty());
        assert!(String::from_utf8_lossy(&result.stderr).contains("memory"));
    }
}

#[test]
fn explain_reports_batch_pipeline() {
    let stdout = output(&[
        "explain",
        "select elb_status_code from it where sent_bytes > 0",
        "--table",
        "it:elb=data/AWSELB.log",
    ]);
    assert!(stdout.contains("Execution pipeline: batch"), "{stdout}");
}

#[test]
fn explain_reports_batch_for_bound_function_projection() {
    let stdout = output(&[
        "explain",
        "select upper(status) from it",
        "--table",
        "it:jsonl=input.jsonl",
    ]);
    assert!(stdout.contains("Execution pipeline: batch"), "{stdout}");
}

#[test]
fn explain_keeps_subquery_projection_in_row_execution() {
    let stdout = output(&[
        "explain",
        "select x, (select count(*) from it) as total from it",
        "--table",
        "it:jsonl=input.jsonl",
    ]);
    assert!(stdout.contains("Execution pipeline: row"), "{stdout}");
    assert!(stdout.contains("unsupported projection expression"), "{stdout}");
}

#[test]
fn explain_names_dynamic_source_row_fallback() {
    let stdout = output(&["explain", "select a from it", "--table", "it:clf=data/structured.log"]);
    assert!(stdout.contains("Execution pipeline: row"), "{stdout}");
    assert!(
        stdout.contains("Batch fallback: DataSource (dynamic format `clf`)"),
        "{stdout}"
    );
}

#[test]
fn explain_reports_batch_for_projected_json() {
    let stdout = output(&["explain", "select a from it", "--table", "it:jsonl=data/structured.log"]);
    assert!(stdout.contains("Execution pipeline: batch"), "{stdout}");
}

#[test]
fn max_memory_stops_materializing_queries_cleanly() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("unique.jsonl");
    let rows: String = (0..200)
        .map(|value| format!("{{\"x\":\"value-{value:04}-{}\"}}\n", "x".repeat(64)))
        .collect();
    std::fs::write(&path, rows).unwrap();
    let table = format!("it:jsonl={}", path.display());

    for query in [
        "select x from it order by x asc",
        "select x from it order by x asc limit 200",
        "select x, count(*) as n from it group by x",
        "select distinct x from it",
    ] {
        let result = Command::new(env!("CARGO_BIN_EXE_logq"))
            .args([
                "query",
                query,
                "--table",
                &table,
                "--output",
                "ndjson",
                "--max-memory",
                "1KiB",
            ])
            .output()
            .unwrap();
        let combined = format!(
            "{}{}",
            String::from_utf8_lossy(&result.stdout),
            String::from_utf8_lossy(&result.stderr)
        );
        assert!(!result.status.success(), "memory ceiling must fail: {query}");
        assert!(String::from_utf8_lossy(&result.stderr).contains("query exceeded memory budget"));
        assert!(
            combined.contains("query exceeded memory budget (--max-memory)"),
            "query: {query}\noutput: {combined}"
        );
    }

    // Each operator stays below this ceiling on its own. The composed query
    // must still fail when DISTINCT and ORDER BY retain more than the shared
    // query budget in aggregate.
    let result = Command::new(env!("CARGO_BIN_EXE_logq"))
        .args([
            "query",
            "select distinct x from it order by x asc",
            "--table",
            &table,
            "--output",
            "ndjson",
            "--max-memory",
            "60KiB",
        ])
        .output()
        .unwrap();
    let combined = format!(
        "{}{}",
        String::from_utf8_lossy(&result.stdout),
        String::from_utf8_lossy(&result.stderr)
    );
    assert!(!result.status.success(), "shared memory ceiling must fail");
    assert!(String::from_utf8_lossy(&result.stderr).contains("query exceeded memory budget"));
    assert!(
        combined.contains("query exceeded memory budget (--max-memory)"),
        "composed query did not share its budget: {combined}"
    );
}
