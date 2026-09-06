use std::io::Write;
use std::process::{Command, Output, Stdio};

fn query(input: &str, table: &str, sql: &str) -> Output {
    let mut child = Command::new(env!("CARGO_BIN_EXE_logq"))
        .args(["query", "--output", "ndjson", "--table", table, sql])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    child.stdin.take().unwrap().write_all(input.as_bytes()).unwrap();
    child.wait_with_output().unwrap()
}

#[cfg(unix)]
fn closed_stdout() -> Stdio {
    let (reader, writer) = std::os::unix::net::UnixStream::pair().unwrap();
    // macOS sets close-on-exec after creating descriptors. A concurrently
    // spawned child can inherit the reader, so dropping it alone is
    // insufficient. Disable writes on the shared endpoint before spawning.
    writer.shutdown(std::net::Shutdown::Write).unwrap();
    drop(reader);
    Stdio::from(std::os::fd::OwnedFd::from(writer))
}

#[cfg(not(unix))]
fn closed_stdout() -> Stdio {
    Stdio::piped()
}

#[test]
fn short_query_output_reports_closed_stdout_in_every_format() {
    for format in ["json", "ndjson", "csv", "table"] {
        let mut child = Command::new(env!("CARGO_BIN_EXE_logq"))
            .args([
                "query",
                "--output",
                format,
                "--table",
                "it:jsonl=stdin",
                "select x from it",
            ])
            .stdin(Stdio::piped())
            .stdout(closed_stdout())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap();
        // Non-Unix platforms use a pipe whose reader is closed before input.
        drop(child.stdout.take());
        child.stdin.take().unwrap().write_all(b"{\"x\":1}\n").unwrap();
        let output = child.wait_with_output().unwrap();
        assert!(!output.status.success(), "{format} silently discarded an output error");
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(!stderr.is_empty(), "{format}: missing output error");
        assert!(!stderr.contains("panicked"), "{format}: {stderr}");
    }
}

#[test]
fn unknown_schema_format_is_a_command_failure() {
    let output = Command::new(env!("CARGO_BIN_EXE_logq"))
        .args(["schema", "invalid"])
        .output()
        .unwrap();
    assert!(!output.status.success());
    assert!(output.stdout.is_empty());
    assert!(String::from_utf8_lossy(&output.stderr).contains("Unknown log format"));
}

#[cfg(unix)]
#[test]
fn schema_reports_stdout_write_failures() {
    for format in ["elb", "alb", "s3", "squid"] {
        let output = Command::new(env!("CARGO_BIN_EXE_logq"))
            .args(["schema", format])
            .stdout(closed_stdout())
            .output()
            .unwrap();
        assert!(!output.status.success(), "{format} silently discarded an output error");
        assert!(!output.stderr.is_empty());
        assert!(!String::from_utf8_lossy(&output.stderr).contains("panicked"));
    }
}

#[test]
fn underscored_table_names_are_queryable() {
    for name in ["access_logs", "_logs2"] {
        let output = query(
            "{\"x\":1}\n",
            &format!("{name}:jsonl=stdin"),
            &format!("select x from {name}"),
        );
        assert!(output.status.success(), "{}", String::from_utf8_lossy(&output.stderr));
        assert_eq!(output.stdout, b"{\"x\":1}\n");
    }
}

#[test]
fn literal_prefixes_do_not_hide_column_names() {
    for name in [
        "true_value",
        "false_value",
        "info",
        "infinite",
        "nanosecond",
        "TRUE值",
        "nullish",
        "missing_value",
    ] {
        let output = query(
            &format!("{{\"{name}\":3}}\n"),
            "it:jsonl=stdin",
            &format!("select {name} as x from it"),
        );
        assert!(
            output.status.success(),
            "{name}: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        assert_eq!(output.stdout, b"{\"x\":3}\n", "{name}");
    }
}

#[test]
fn scientific_literals_are_consumed_as_one_number() {
    for number in ["1e3", "1E+3", "10000e-1"] {
        let output = query("{}\n", "it:jsonl=stdin", &format!("select {number} as n from it"));
        assert!(
            output.status.success(),
            "{number}: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        let value: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
        assert_eq!(value["n"].as_f64(), Some(1000.0));
    }
}

#[test]
fn zero_argument_scalar_functions_do_not_panic_during_planning() {
    let output = query("{}\n", "it:jsonl=stdin", "select pi() as p from it");
    assert!(output.status.success(), "{}", String::from_utf8_lossy(&output.stderr));
    let value: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert!((value["p"].as_f64().unwrap() - std::f64::consts::PI).abs() < 0.000001);
}

#[test]
fn aggregates_reject_invalid_arity_and_percentiles_at_planning_time() {
    for expression in [
        "count()",
        "sum(1, 2)",
        "avg()",
        "upper()",
        "percentile_disc(2.0) within group (order by x asc)",
        "approx_percentile(-0.1) within group (order by x asc)",
    ] {
        let output = Command::new(env!("CARGO_BIN_EXE_logq"))
            .args([
                "explain",
                "--table",
                "it:jsonl=unused.jsonl",
                &format!("select {expression} from it"),
            ])
            .output()
            .unwrap();
        assert!(!output.status.success(), "accepted {expression}");
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(stderr.contains("Invalid Arguments"), "{expression}: {stderr}");
        assert!(!stderr.contains("panicked"), "{expression}: {stderr}");
    }
}

#[test]
fn aggregate_names_are_case_insensitive() {
    for expression in [
        "COUNT(*)",
        "SuM(x)",
        "AVG(x)",
        "MIN(x)",
        "MAX(x)",
        "PERCENTILE_DISC(0.0) within group (order by x asc)",
    ] {
        let output = query(
            "{\"x\":1}\n",
            "it:jsonl=stdin",
            &format!("select {expression} as n from it"),
        );
        assert!(
            output.status.success(),
            "{expression}: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        let value: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
        assert_eq!(value["n"].as_f64(), Some(1.0), "{expression}");
    }
}

#[test]
fn non_boolean_arithmetic_predicates_report_errors_without_panicking() {
    for predicate in ["1 + 2", "x * 2", "true and x / 2"] {
        let output = query(
            "{\"x\":1}\n",
            "it:jsonl=stdin",
            &format!("select x from it where {predicate}"),
        );
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(!output.status.success(), "accepted {predicate}");
        assert!(!stderr.contains("panicked"), "{predicate}: {stderr}");
        assert!(stderr.contains("Type Mismatch"), "{predicate}: {stderr}");
    }
}

#[test]
fn cast_and_postfix_predicates_compose_with_spaced_operators() {
    for predicate in [
        "cast(x as int) > 0",
        "x is not null and x > 0",
        "cast(x as int) in (1, 2) and true",
        "case when x > 0 then true else false end and true",
    ] {
        let output = query(
            "{\"x\":1}\n",
            "it:jsonl=stdin",
            &format!("select x from it where {predicate}"),
        );
        assert!(
            output.status.success(),
            "{predicate}: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        assert_eq!(output.stdout, b"{\"x\":1}\n", "{predicate}");
    }
}
