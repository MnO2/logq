use serde_json::{Value, json};
use std::process::{Command, Output, Stdio};

fn run(input: &str, sql: &str, threads: usize, row: bool) -> Output {
    run_format(input, sql, threads, row, "jsonl")
}

fn run_format(input: &str, sql: &str, threads: usize, row: bool, format: &str) -> Output {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("input.jsonl");
    std::fs::write(&path, input).unwrap();
    let source = if row {
        "stdin".into()
    } else {
        path.display().to_string()
    };
    let input = if row {
        Stdio::from(std::fs::File::open(&path).unwrap())
    } else {
        Stdio::null()
    };
    Command::new(env!("CARGO_BIN_EXE_logq"))
        .args([
            "query",
            "--output",
            "ndjson",
            "--threads",
            &threads.to_string(),
            "--table",
        ])
        .arg(format!("it:{format}={source}"))
        .arg(sql)
        .stdin(input)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()
        .unwrap()
}

fn values(output: Output) -> Vec<Value> {
    assert!(output.status.success(), "{}", String::from_utf8_lossy(&output.stderr));
    String::from_utf8(output.stdout)
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect()
}

fn assert_batch(sql: &str) {
    let output = Command::new(env!("CARGO_BIN_EXE_logq"))
        .args(["explain", "--table", "it:jsonl=input.jsonl", sql])
        .output()
        .unwrap();
    assert!(output.status.success(), "{}", String::from_utf8_lossy(&output.stderr));
    let plan = String::from_utf8(output.stdout).unwrap();
    assert!(plan.contains("Execution pipeline: batch"), "{sql}: {plan}");
}

#[test]
fn arithmetic_cast_coalesce_and_string_projections_stay_batch() {
    let input = "{\"x\":2,\"label\":\"Alpha\"}\n{\"x\":null,\"label\":null}\n{}\n";
    let sql = "select x + 1 as n, cast(x as float) as f, coalesce(x, 7) as c, upper(label) as label from it";
    assert_batch(sql);
    let expected = vec![
        json!({"n":3,"f":2,"c":2,"label":"ALPHA"}),
        json!({"n":null,"f":null,"c":7,"label":null}),
        json!({"n":null,"f":null,"c":7,"label":null}),
    ];
    assert_eq!(values(run(input, sql, 1, true)), expected);
    for threads in [1, 4] {
        assert_eq!(values(run(input, sql, threads, false)), expected);
    }
}

#[test]
fn float_plus_chains_match_rows_across_masks_aliases_and_batch_type_changes() {
    // Decimal spelling makes the first complete batch Float32, including 2^24
    // where sixteen rounded +0.5 steps differ from one folded +8 operation.
    let mut input = String::new();
    for row in 0..2200 {
        input.push_str(match row % 6 {
            0 => "{\"v\":16777216.0,\"keep\":true}\n",
            1 => "{\"v\":1.25,\"keep\":true}\n",
            2 => "{\"v\":null,\"keep\":true}\n",
            3 => "{\"keep\":true}\n",
            4 => "{\"v\":-2.5,\"keep\":false}\n",
            _ => "{\"v\":-0.0,\"keep\":true}\n",
        });
    }
    // Later batches can change to Mixed/Int32 and must conservatively use the
    // scalar evaluator without stale runtime type assumptions.
    input.push_str("{\"v\":3,\"keep\":true}\n{\"v\":\"bad\",\"keep\":false}\n");
    let chain = format!("v{}", " + 0.5".repeat(16));
    for sql in [
        format!("select {chain} as n, v + 1.25 as other from it where keep = true"),
        format!("select v as n, v + 1.25 as other, {chain} as n from it where keep = true"),
        format!("select sum({chain}) as n, count({chain}) as c from it where keep = true"),
        format!("select {chain} as n from it where keep = true order by n asc limit 8"),
    ] {
        assert_batch(&sql);
        let expected = run(&input, &sql, 1, true);
        assert!(
            expected.status.success(),
            "{}",
            String::from_utf8_lossy(&expected.stderr)
        );
        for threads in [1, 4] {
            let actual = run(&input, &sql, threads, false);
            assert!(actual.status.success(), "{}", String::from_utf8_lossy(&actual.stderr));
            assert_eq!(actual.stdout, expected.stdout, "{sql}, threads={threads}");
        }
    }
    let first = values(run(
        "{\"v\":16777216.0}\n",
        &format!("select {chain} as n from it"),
        1,
        false,
    ));
    assert_eq!(first, vec![json!({"n":16777216})]);
}

#[test]
fn float_plus_chains_preserve_prefix_limit_and_overwritten_errors() {
    let input = "{\"v\":1.25,\"x\":\"12\",\"keep\":true}\n{\"v\":2.5,\"x\":\"bad\",\"keep\":true}\n";
    for sql in [
        "select v + 0.5 as n, cast(x as int) as x from it limit 1",
        "select v + 0.5 as n, cast(x as int) as x from it where keep = true limit 1",
        "select cast(x as int) as n, v + 0.5 as n from it",
        "select v + 0.5 as n, cast(x as int) as n from it",
    ] {
        let expected = run(input, sql, 1, true);
        for threads in [1, 4] {
            let actual = run(input, sql, threads, false);
            assert_eq!(actual.status.success(), expected.status.success(), "{sql}");
            if expected.status.success() {
                assert_eq!(actual.stdout, expected.stdout, "{sql}");
            } else {
                assert_eq!(actual.stderr, expected.stderr, "{sql}");
            }
        }
    }
}

#[test]
fn case_and_coalesce_only_evaluate_selected_branches() {
    let input = "{\"x\":4,\"keep\":true}\n{\"x\":9,\"keep\":false}\n{\"x\":null}\n";
    for sql in [
        "select case when keep then x else 0 end as n from it",
        "select case when x > 5 then 1 when x is null then 2 else 3 end as n from it",
        "select coalesce(x, cast(\"bad\" as int)) as n from it where x is not null",
    ] {
        assert_batch(sql);
        let expected = values(run(input, sql, 1, true));
        for threads in [1, 4] {
            assert_eq!(values(run(input, sql, threads, false)), expected, "{sql}");
        }
    }
}

#[test]
fn bound_predicate_logic_preserves_null_missing_and_short_circuit() {
    let input = "{\"x\":1,\"flag\":true}\n{\"x\":null,\"flag\":false}\n{}\n";
    for sql in [
        "select case when flag and x > 0 then 1 else 0 end as n from it",
        "select case when flag or x is missing then 1 else 0 end as n from it",
        "select case when x in (1, null) then 1 else 0 end as n from it",
        "select case when x not in (2, null) then 1 else 0 end as n from it",
    ] {
        assert_batch(sql);
        let expected = values(run(input, sql, 1, true));
        assert_eq!(values(run(input, sql, 4, false)), expected, "{sql}");
    }
}

#[test]
fn projected_nested_objects_arrays_and_duplicate_alias_order_match_rows() {
    let input = "{\"obj\":{\"name\":\"a\"},\"arr\":[4],\"x\":2}\n{\"obj\":null,\"arr\":[],\"x\":3}\n";
    for sql in [
        "select upper(obj.name) as name, arr[0] + 1 as first, obj as obj from it",
        "select x + 1 as a, x + 2 as b, x + 3 as a from it",
        "select x + 1 as x, x + 2 as other from it",
    ] {
        assert_batch(sql);
        let expected = run(input, sql, 1, true);
        assert!(
            expected.status.success(),
            "{}",
            String::from_utf8_lossy(&expected.stderr)
        );
        let actual = run(input, sql, 4, false);
        assert!(actual.status.success(), "{}", String::from_utf8_lossy(&actual.stderr));
        assert_eq!(actual.stdout, expected.stdout, "{sql}");
    }
}

#[test]
fn projection_errors_are_not_evaluated_for_filtered_rows_or_erased_by_aliases() {
    let input = "{\"x\":\"12\",\"keep\":true}\n{\"x\":\"bad\",\"keep\":false}\n";
    let sql = "select cast(x as int) as n from it where keep = true";
    assert_batch(sql);
    assert_eq!(values(run(input, sql, 4, false)), vec![json!({"n":12})]);
    for sql in [
        "select cast(x as int) as n from it",
        "select cast(x as int) as n, 1 as n from it",
        "select x + 1 as n from it",
    ] {
        let expected = run(input, sql, 1, true);
        assert!(!expected.status.success(), "{sql}");
        for threads in [1, 4] {
            let actual = run(input, sql, threads, false);
            assert!(!actual.status.success(), "{sql}");
            assert_eq!(actual.stderr, expected.stderr, "{sql}");
        }
    }
}

#[test]
fn aggregate_expressions_consume_materialized_projection_once() {
    let input = "{\"x\":1,\"g\":\"a\"}\n{\"x\":2,\"g\":\"a\"}\n{\"x\":4,\"g\":\"b\"}\n{\"x\":null,\"g\":\"b\"}\n";
    for (sql, expected) in [
        (
            "select sum(x + 1) as n, avg(x + 1) as a, count(x + 1) as c from it",
            vec![json!({"n":10,"a":3.3333333,"c":3})],
        ),
        (
            "select g, sum(x + 1) as n, count(*) as c from it group by g order by g asc",
            vec![json!({"g":"a","n":5,"c":2}), json!({"g":"b","n":5,"c":2})],
        ),
    ] {
        assert_batch(sql);
        for row in [true, false] {
            for threads in [1, 4] {
                let actual = values(run(input, sql, threads, row));
                assert_eq!(actual, expected, "{sql}, row={row}, threads={threads}");
            }
        }
    }
}

#[test]
fn batch_expressions_preserve_values_across_multiple_batches() {
    let input: String = (0..2500).map(|x| format!("{{\"x\":{x}}}\n")).collect();
    let sql = "select x + 1 as n from it where x > 1000";
    assert_batch(sql);
    let expected = values(run(&input, sql, 1, true));
    assert_eq!(values(run(&input, sql, 4, false)), expected);
}

#[test]
fn limit_masks_projection_errors_after_the_last_requested_active_row() {
    let input = "{\"x\":\"skip\",\"keep\":false}\n{\"x\":\"12\",\"keep\":true}\n{\"x\":\"bad\",\"keep\":true}\n";
    for sql in [
        "select cast(x as int) as n from it where keep = true limit 1",
        "select cast(x as int) as n from it limit 0",
    ] {
        let expected = values(run(input, sql, 1, true));
        for threads in [1, 4] {
            assert_eq!(values(run(input, sql, threads, false)), expected, "{sql}");
        }
    }
    // Blocking operators still require every input expression to be evaluated.
    for sql in [
        "select cast(x as int) as n from it where keep = true order by n asc limit 1",
        "select sum(cast(x as int)) as n from it where keep = true limit 1",
    ] {
        assert!(!run(input, sql, 4, false).status.success(), "{sql}");
    }
}

#[test]
fn fixed_format_expressions_preserve_quoted_strings_and_row_parse_errors() {
    let sql = "select date_part(\"minute\", timestamp) as minute, time_bucket(\"5m\", timestamp) as bucket, upper(user_agent) as agent from it";
    let output = Command::new(env!("CARGO_BIN_EXE_logq"))
        .args(["explain", "--table", "it:elb=data/AWSELB.log", sql])
        .output()
        .unwrap();
    assert!(output.status.success(), "{}", String::from_utf8_lossy(&output.stderr));
    let explain = String::from_utf8(output.stdout).unwrap();
    assert!(explain.contains("Execution pipeline: row"), "{explain}");
    assert!(explain.contains("fixed-format row values are preserved"), "{explain}");
    let input = std::fs::read_to_string("data/AWSELB.log").unwrap();
    let expected = values(run_format(&input, sql, 1, true, "elb"));
    for threads in [1, 4] {
        let actual = values(run_format(&input, sql, threads, false, "elb"));
        assert_eq!(actual.len(), expected.len());
        for (row, (actual, expected)) in actual.iter().zip(&expected).enumerate() {
            assert_eq!(actual, expected, "row {row}");
        }
    }
    let malformed = "2019-06-07T18:45:31Z elb1 1.1.1.1:1 2.2.2.2:2 0 0 0 200 200 0 bad \"GET https://example.com/ HTTP/1.1\" \"agent\" c t\n";
    for row in [true, false] {
        assert!(
            !run_format(malformed, "select sent_bytes + 1 as n from it", 4, row, "elb")
                .status
                .success()
        );
    }
}

#[test]
fn json_time_functions_run_in_bound_batches() {
    let input = "{\"ts\":0}\n{\"ts\":301}\n{\"ts\":null}\n{}\n";
    let sql = "select date_part(\"minute\", from_unixtime(ts)) as minute, time_bucket(\"5m\", from_unixtime(ts)) as bucket from it";
    assert_batch(sql);
    let expected = values(run(input, sql, 1, true));
    assert_eq!(expected[0], json!({"minute":0,"bucket":"1970-01-01 00:00:00 +00:00"}));
    for threads in [1, 4] {
        assert_eq!(values(run(input, sql, threads, false)), expected);
    }
}

#[test]
fn expression_prefix_limit_does_not_parse_unrequested_json_suffix() {
    for (input, sql) in [
        ("{\"x\":1}\n{bad}\n", "select x + 1 as n from it limit 1"),
        (
            "{\"x\":0}\n{\"x\":1}\n{bad}\n",
            "select x + 1 as n from it where x > 0 limit 1",
        ),
        ("{\"x\":1}\n{bad}\n", "select distinct x + 1 as n from it limit 1"),
    ] {
        let explain = Command::new(env!("CARGO_BIN_EXE_logq"))
            .args(["explain", "--table", "it:jsonl=input.jsonl", sql])
            .output()
            .unwrap();
        let plan = String::from_utf8(explain.stdout).unwrap();
        assert!(plan.contains("Execution pipeline: row"), "{sql}: {plan}");
        assert!(plan.contains("prefix LIMIT"), "{sql}: {plan}");
        for threads in [1, 4] {
            assert_eq!(values(run(input, sql, threads, false)), vec![json!({"n":2})], "{sql}");
        }
    }
    let sql = "select x + 1 as n from it order by n asc limit 1";
    assert_batch(sql);
    assert!(!run("{\"x\":1}\n{bad}\n", sql, 4, false).status.success());
}
