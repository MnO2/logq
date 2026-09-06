use serde_json::{Value, json};
use std::process::{Command, Output, Stdio};

fn query(input: &str, sql: &str, row: bool, memory: bool) -> Output {
    query_format(input, sql, row, memory, "jsonl")
}

fn query_format(input: &str, sql: &str, row: bool, memory: bool, format: &str) -> Output {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("input.jsonl");
    std::fs::write(&path, input).unwrap();
    let source = if row {
        "stdin".into()
    } else {
        path.display().to_string()
    };
    let mut command = Command::new(env!("CARGO_BIN_EXE_logq"));
    command
        .args(["query", sql, "--output", "json", "--threads", "4", "--table"])
        .arg(format!("it:{format}={source}"));
    if row {
        command.stdin(Stdio::from(std::fs::File::open(path).unwrap()));
    }
    if memory {
        command.args(["--max-memory", "1MiB"]);
    }
    command.output().unwrap()
}

fn assert_query(input: &str, sql: &str, expected: Value) {
    for row in [true, false] {
        for memory in [true, false] {
            let output = query(input, sql, row, memory);
            assert!(
                output.status.success(),
                "{sql}: {}",
                String::from_utf8_lossy(&output.stderr)
            );
            let actual: Value = serde_json::from_slice(&output.stdout).unwrap();
            assert_eq!(actual, expected, "{sql}, row={row}, memory={memory}");
        }
    }
}

const INPUT: &str =
    "{\"k\":\"a\",\"x\":1}\n{\"k\":\"a\",\"x\":3}\n{\"k\":\"b\",\"x\":7}\n{\"k\":\"c\",\"x\":null}\n{\"k\":\"c\"}\n";

#[test]
fn having_aggregate_calls_reuse_selected_results_and_keep_aliases() {
    assert_query(
        INPUT,
        "select k, count(*) as n from it group by k having COUNT(*) > 1 order by k asc",
        json!([{"k":"a","n":2},{"k":"c","n":2}]),
    );
    assert_query(
        INPUT,
        "select k, count(*) from it group by k having count(*) > 1 order by k asc",
        json!([{"k":"a","_2":2},{"k":"c","_2":2}]),
    );
    assert_query(
        INPUT,
        "select k, count(*) as n from it group by k having n > 1 order by k asc",
        json!([{"k":"a","n":2},{"k":"c","n":2}]),
    );
}

#[test]
fn having_hidden_aggregates_do_not_appear_in_results() {
    assert_query(
        INPUT,
        "select k from it group by k having count(*) > 1 order by k asc",
        json!([{"k":"a"},{"k":"c"}]),
    );
    assert_query(
        INPUT,
        "select k, count(*) as n from it group by k having sum(x) > 3 and avg(x) < 5 order by k asc",
        json!([{"k":"a","n":2}]),
    );
    assert_query(
        INPUT,
        "select distinct k from it group by k having count(*) > 1 order by k desc limit 1",
        json!([{"k":"c"}]),
    );
}

#[test]
fn having_aggregates_support_wrappers_case_nulls_and_group_keys() {
    assert_query(
        INPUT,
        "select k from it group by k having abs(SUM(x)) >= 4 and k != \"b\" order by k asc",
        json!([{"k":"a"}]),
    );
    assert_query(
        INPUT,
        "select k from it group by k having sum(x) is null order by k asc",
        json!([{"k":"c"}]),
    );
    assert_query(
        INPUT,
        "select k from it group by k having case when count(x) = 0 then true else false end order by k asc",
        json!([{"k":"c"}]),
    );
    assert_query(
        INPUT,
        "select k from it group by k having coalesce(sum(x), 0) = 0 order by k asc",
        json!([{"k":"c"}]),
    );
}

#[test]
fn having_global_aggregates_preserve_empty_input_and_filtering() {
    assert_query(
        "{\"x\":1}\n{\"x\":2}\n",
        "select count(*) as n from it having sum(x + 1) = 5.0",
        json!([{"n":2}]),
    );
    assert_query(
        INPUT,
        "select count(*) as n from it having count(*) > 0",
        json!([{"n":5}]),
    );
    assert_query("", "select count(*) as n from it having count(*) > 0", json!([]));
    assert_query(
        "",
        "select count(*) as n from it having sum(x) is null",
        json!([{"n":0}]),
    );
}

#[test]
fn having_internal_names_and_duplicate_aliases_do_not_collide() {
    assert_query(
        "{\"k\":\"a\",\"__logq_having_1\":5}\n",
        "select k from it group by k having sum(__logq_having_1) > 3",
        json!([{"k":"a"}]),
    );
    assert_query(
        INPUT,
        "select k, count(*) as __logq_having_1 from it group by k having sum(x) > 3 order by k asc",
        json!([{"k":"a","__logq_having_1":2},{"k":"b","__logq_having_1":1}]),
    );
    assert_query(
        INPUT,
        "select k, count(*) as n, sum(x) as n from it group by k having count(*) > 1 order by k asc",
        json!([{"k":"a","n":4},{"k":"c","n":null}]),
    );
}

#[test]
fn having_reused_default_names_and_hidden_fields_pass_fixed_schema_validation() {
    let input = "2019-06-07T18:45:31Z elb1 1.1.1.1:1 2.2.2.2:2 0 0 0 200 200 0 10 \"GET https://example.com/ HTTP/1.1\" \"agent\" c t\n";
    for (sql, expected) in [
        ("select count(*) from it having count(*) > 0", json!([{"_1":1}])),
        (
            "select count(*) as n from it having sum(sent_bytes) > 3",
            json!([{"n":1}]),
        ),
    ] {
        for row in [true, false] {
            let output = query_format(input, sql, row, true, "elb");
            assert!(
                output.status.success(),
                "{sql}: {}",
                String::from_utf8_lossy(&output.stderr)
            );
            assert_eq!(serde_json::from_slice::<Value>(&output.stdout).unwrap(), expected);
        }
    }
}

#[test]
fn duplicate_aggregate_aliases_sort_by_the_visible_last_value() {
    let input = "{\"k\":\"a\",\"x\":1}\n{\"k\":\"a\",\"x\":3}\n{\"k\":\"b\",\"x\":7}\n";
    assert_query(
        input,
        "select k, count(*) as n, sum(x) as n from it group by k order by n desc",
        json!([{"k":"b","n":7},{"k":"a","n":4}]),
    );
}

#[test]
fn duplicate_aggregate_aliases_top_n_keeps_a_complete_output_schema() {
    let input = "{\"k\":\"a\",\"x\":1}\n{\"k\":\"a\",\"x\":3}\n{\"k\":\"b\",\"x\":7}\n";
    assert_query(
        input,
        "select k, count(*) as n, sum(x) as n from it group by k order by n desc limit 1",
        json!([{"k":"b","n":7}]),
    );
}

#[test]
fn having_aggregate_errors_are_rejected_without_panics() {
    for sql in [
        "select k from it group by k having count() > 0",
        "select k from it group by k having sum(x, x) > 0",
        "select k from it group by k having sum(count(*)) > 0",
        "select k from it group by k having count(sum(x)) > 0",
        "select k from it group by k having no_such_function(sum(x)) > 0",
    ] {
        let output = query(INPUT, sql, false, false);
        assert!(!output.status.success(), "{sql} unexpectedly succeeded");
        let error = String::from_utf8_lossy(&output.stderr);
        assert!(!error.contains("panicked"), "{sql}: {error}");
    }
}

#[test]
fn having_only_global_aggregation_does_not_silently_discard_selected_values() {
    for sql in [
        "select x from it having count(*) > 0",
        "select 1 as one from it having count(*) > 0",
    ] {
        let output = query(INPUT, sql, false, false);
        assert!(!output.status.success(), "{sql} silently discarded SELECT fields");
        assert!(String::from_utf8_lossy(&output.stderr).contains("explicit GROUP BY or an aggregate in SELECT"));
    }
}

#[test]
fn aggregate_inputs_do_not_overwrite_group_aliases() {
    let input = "{\"x\":\"a\",\"k\":1}\n{\"x\":\"a\",\"k\":2}\n";
    assert_query(
        input,
        "select k from it group by x as k having sum(k) > 0",
        json!([{"k":"a"}]),
    );
    assert_query(
        input,
        "select k, sum(k) as total, count(k) as n, avg(k) as mean, min(k) as lo, max(k) as hi, first(k) as first_value, last(k) as last_value from it group by x as k",
        json!([{"k":"a","total":3,"n":2,"mean":1.5,"lo":1,"hi":2,"first_value":1,"last_value":2}]),
    );
    assert_query(
        input,
        "select k, percentile_disc(1.0) within group (order by k asc) as hi from it group by x as k",
        json!([{"k":"a","hi":2}]),
    );
}

#[test]
fn aggregate_private_inputs_preserve_anonymous_names_and_raw_name_collisions() {
    let input =
        "{\"x\":\"a\",\"k\":1,\"__logq_aggregate_input_1\":7}\n{\"x\":\"a\",\"k\":2,\"__logq_aggregate_input_1\":11}\n";
    assert_query(
        input,
        "select k, sum(k), sum(__logq_aggregate_input_1) as other from it group by x as k",
        json!([{"k":"a","_2":3,"other":18}]),
    );
    assert_query(
        input,
        "select _1, sum(k + 1) as total from it group by x as _1 having sum(k + 1) > 0",
        json!([{"_1":"a","total":5}]),
    );
}

#[test]
fn aggregates_read_projected_nested_and_array_arguments() {
    let input = "{\"n\":{\"v\":1},\"items\":[2]}\n{\"n\":{\"v\":3},\"items\":[4]}\n{\"n\":{\"v\":null},\"items\":[null]}\n{\"n\":{},\"items\":[]}\n{}\n";
    assert_query(
        input,
        "select avg(n.v) as mean, min(n.v) as lo, max(n.v) as hi, count(n.v) as n, first(n.v) as first_value, last(n.v) as last_value, approx_count_distinct(n.v) as distinct_values from it",
        json!([{"mean":2,"lo":1,"hi":3,"n":2,"first_value":1,"last_value":null,"distinct_values":2}]),
    );
    assert_query(
        input,
        "select count(*) as n from it having avg(n.v) > 0",
        json!([{"n":5}]),
    );
    assert_query(
        input,
        "select avg(items[0]) as mean, sum(items[0]) as total from it",
        json!([{"mean":3,"total":6}]),
    );
    assert_query(
        input,
        "select avg(n.v as v_input) as mean, min(items[0] as item_input) as lo from it",
        json!([{"mean":2,"lo":2}]),
    );
}
