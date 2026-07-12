use std::process::Command;

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
fn explain_names_complex_projection_row_fallback() {
    let stdout = output(&[
        "explain",
        "select upper(elb_status_code) from it",
        "--table",
        "it:elb=data/AWSELB.log",
    ]);
    assert!(stdout.contains("Execution pipeline: row"), "{stdout}");
    assert!(
        stdout.contains("Batch fallback: Map (complex projection expression)"),
        "{stdout}"
    );
}

#[test]
fn explain_names_dynamic_source_row_fallback() {
    let stdout = output(&["explain", "select a from it", "--table", "it:jsonl=data/structured.log"]);
    assert!(stdout.contains("Execution pipeline: row"), "{stdout}");
    assert!(
        stdout.contains("Batch fallback: DataSource (dynamic format `jsonl`)"),
        "{stdout}"
    );
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
    assert!(
        combined.contains("query exceeded memory budget (--max-memory)"),
        "composed query did not share its budget: {combined}"
    );
}
