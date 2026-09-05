use serde_json::{Value, json};
use std::process::Command;

fn query_elb(query: &str, memory: Option<&str>) -> Vec<Value> {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("input.log");
    std::fs::write(&path, concat!(
        "2019-06-07T18:45:31Z elb1 1.1.1.1:1 2.2.2.2:2 0 0 0 200 200 0 10 \"GET https://example.com/ HTTP/1.1\" \"agent\" c t\n",
        "2019-06-07T18:45:32Z elb1 1.1.1.1:1 2.2.2.2:2 0 0 0 503 503 0 20 \"GET https://example.com/ HTTP/1.1\" \"agent\" c t arn trace\n",
        "2019-06-07T18:45:33Z elb1 1.1.1.1:1 2.2.2.2:2 0 0 0 200 200 0 30 \"GET https://example.com/ HTTP/1.1\" \"agent\" c t\n",
    )).unwrap();
    let mut command = Command::new(env!("CARGO_BIN_EXE_logq"));
    command.args(["query", "--output", "ndjson", "--table"]);
    command.arg(format!("it:elb={}", path.display()));
    if let Some(memory) = memory {
        command.args(["--max-memory", memory]);
    }
    let output = command.arg(query).output().unwrap();
    assert!(output.status.success(), "{}", String::from_utf8_lossy(&output.stderr));
    String::from_utf8(output.stdout)
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str(line).unwrap_or_else(|error| panic!("{query}: {line}: {error}")))
        .collect()
}

#[test]
fn count_column_excludes_absent_values_and_preserves_alias() {
    for filter in ["", " where sent_bytes > 0"] {
        let query = format!("select count(target_group_arn) as n from it{filter}");
        assert_eq!(query_elb(&query, None), vec![json!({"n":1})]);
        assert_eq!(query_elb(&query, Some("1MiB")), vec![json!({"n":1})]);
    }
}

#[test]
fn scan_count_star_preserves_alias_and_empty_result() {
    assert_eq!(query_elb("select count(*) as n from it", None), vec![json!({"n":3})]);
    assert_eq!(
        query_elb("select count(*) as n from it where sent_bytes < 0", None),
        vec![json!({"n":0})]
    );
}

#[test]
fn batch_group_then_sort_and_top_n_preserve_schema() {
    for limit in ["", " limit 2"] {
        let query = format!(
            "select elb_status_code, count(*) as n from it where true group by elb_status_code order by n desc{limit}"
        );
        let expected = vec![
            json!({"elb_status_code":"200","n":2}),
            json!({"elb_status_code":"503","n":1}),
        ];
        assert_eq!(query_elb(&query, None), expected);
        assert_eq!(query_elb(&query, Some("1MiB")), expected);
    }
}

#[test]
fn batch_grouping_preserves_null_keys() {
    let query = "select target_group_arn, count(*) as n from it where true group by target_group_arn order by n desc";
    let expected = vec![
        json!({"target_group_arn":null,"n":2}),
        json!({"target_group_arn":"arn","n":1}),
    ];
    assert_eq!(query_elb(query, Some("1MiB")), expected);
    assert_eq!(query_elb(query, None), expected);
}

#[test]
fn grouped_count_and_redundant_filter_choose_batch() {
    for predicate in ["", " where true"] {
        let query = format!("select elb_status_code, count(*) as n from it{predicate} group by elb_status_code");
        let output = Command::new(env!("CARGO_BIN_EXE_logq"))
            .args(["explain", "--table", "it:elb=data/AWSELB.log", &query])
            .output()
            .unwrap();
        assert!(output.status.success());
        assert!(
            String::from_utf8(output.stdout)
                .unwrap()
                .contains("Execution pipeline: batch"),
            "{query}"
        );
    }
}

#[test]
fn batch_projection_preserves_aliases_and_repeated_sources() {
    let query = "select sent_bytes as a, sent_bytes as b from it where true order by a desc limit 2";
    for budget in [None, Some("1MiB")] {
        assert_eq!(
            query_elb(query, budget),
            vec![json!({"a":30,"b":30}), json!({"a":20,"b":20})]
        );
    }
}

fn query_json(input: &str, query: &str, threads: &str) -> std::process::Output {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("input.jsonl");
    std::fs::write(&path, input).unwrap();
    Command::new(env!("CARGO_BIN_EXE_logq"))
        .args(["query", "--output", "ndjson", "--threads", threads, "--table"])
        .arg(format!("it:jsonl={}", path.display()))
        .arg(query)
        .output()
        .unwrap()
}

#[test]
fn projected_json_chooses_batch_and_preserves_dynamic_values() {
    let query = "select x as a, x as b from it where true";
    let explain = Command::new(env!("CARGO_BIN_EXE_logq"))
        .args(["explain", "--table", "it:jsonl=input.jsonl", query])
        .output()
        .unwrap();
    assert!(
        String::from_utf8(explain.stdout)
            .unwrap()
            .contains("Execution pipeline: batch")
    );
    for threads in ["1", "4"] {
        let output = query_json(
            "{\"x\":1,\"unused\":[1,2]}\n{\"x\":null}\n{}\n{\"x\":{\"a\":2}}\n",
            query,
            threads,
        );
        assert!(output.status.success(), "{}", String::from_utf8_lossy(&output.stderr));
        let values: Vec<Value> = String::from_utf8(output.stdout)
            .unwrap()
            .lines()
            .map(|s| serde_json::from_str(s).unwrap())
            .collect();
        assert_eq!(
            values,
            vec![
                json!({"a":1,"b":1}),
                json!({"a":null,"b":null}),
                json!({"a":null,"b":null}),
                json!({"a":{"a":2},"b":{"a":2}})
            ]
        );
    }
}

#[test]
fn projected_json_validates_unused_fields_and_keeps_nested_dependencies() {
    for threads in ["1", "4"] {
        for malformed in [
            "{\"x\":1,\"unused\":[1,]}",
            "{\"x\":1,\"unused\":1e999}",
            "{\"x\":1} trailing",
        ] {
            let output = query_json(malformed, "select count(x) from it", threads);
            assert!(!output.status.success(), "invalid ignored JSON accepted: {malformed}");
        }
        let output = query_json(
            "{\"nested\":{\"name\":\"alice\"},\"keep\":true,\"unused\":1}\n",
            "select upper(nested.name) as name from it where keep = true",
            threads,
        );
        assert!(output.status.success(), "{}", String::from_utf8_lossy(&output.stderr));
        assert_eq!(
            serde_json::from_slice::<Value>(&output.stdout).unwrap(),
            json!({"name":"ALICE"})
        );
    }
}

#[test]
fn generous_memory_ceiling_keeps_batch_and_reports_auto_threads() {
    let output = Command::new(env!("CARGO_BIN_EXE_logq"))
        .args([
            "explain",
            "--max-memory",
            "1GiB",
            "--threads",
            "0",
            "--table",
            "it:elb=data/AWSELB.log",
            "select elb_status_code, count(*) as n from it group by elb_status_code",
        ])
        .output()
        .unwrap();
    assert!(output.status.success());
    let text = String::from_utf8(output.stdout).unwrap();
    assert!(text.contains("Execution pipeline: batch"), "{text}");
    let resolved = std::thread::available_parallelism().map_or(1, |n| n.get());
    assert!(text.contains(&format!("Resolved thread limit: {resolved}")), "{text}");
}

#[test]
fn all_end_to_end_benchmark_queries_parse_completely() {
    let catalog: Value = serde_json::from_str(include_str!("../scripts/bench_e2e/queries.json")).unwrap();
    for query in catalog["queries"].as_array().unwrap() {
        let sql = query["logq"].as_str().unwrap();
        let output = Command::new(env!("CARGO_BIN_EXE_logq"))
            .args(["explain", "--table", "it:jsonl=input.jsonl", sql])
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "{sql}: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    }
}

#[test]
fn array_root_projection_and_nested_sort_keep_dependencies() {
    let output = query_json(
        "{\"x\":[9]}\n{\"x\":[3]}\n",
        "select x[0] as v from it where x[0] = 9",
        "1",
    );
    assert!(output.status.success(), "{}", String::from_utf8_lossy(&output.stderr));
    assert_eq!(String::from_utf8(output.stdout).unwrap().trim(), "{\"v\":9}");
    for limit in ["", " limit 2"] {
        let output = query_json(
            "{\"obj\":{\"n\":2}}\n{\"obj\":{\"n\":1}}\n",
            &format!("select obj from it order by obj.n asc{limit}"),
            "1",
        );
        assert!(output.status.success());
        let values: Vec<Value> = String::from_utf8(output.stdout)
            .unwrap()
            .lines()
            .map(|line| serde_json::from_str(line).unwrap())
            .collect();
        assert_eq!(values, vec![json!({"obj":{"n":1}}), json!({"obj":{"n":2}})]);
    }
}

#[test]
fn batch_projection_preserves_scope_constants_and_duplicate_output_order() {
    let output = query_json(
        "{\"x\":1,\"y\":2}\n",
        "select 7 as n, true as flag, \"ok\" as label from it",
        "1",
    );
    assert!(output.status.success());
    assert_eq!(
        serde_json::from_slice::<Value>(&output.stdout).unwrap(),
        json!({"n":7,"flag":true,"label":"ok"})
    );
    let output = query_json("{\"x\":1,\"y\":2}\n", "select x as a, y as b, x as a from it", "1");
    assert!(output.status.success());
    assert_eq!(String::from_utf8(output.stdout).unwrap().trim(), "{\"b\":2,\"a\":1}");
}

#[test]
fn sql_time_bucket_uses_batch_and_preserves_aggregate_dependencies() {
    let query = "select time_bucket(\"5m\", timestamp) as bucket, sum(sent_bytes) as n from it group by time_bucket(\"5m\", timestamp) as bucket";
    let output = Command::new(env!("CARGO_BIN_EXE_logq"))
        .args(["explain", "--table", "it:elb=data/AWSELB.log", query])
        .output()
        .unwrap();
    assert!(output.status.success());
    assert!(
        String::from_utf8(output.stdout)
            .unwrap()
            .contains("Execution pipeline: batch")
    );
    for memory in [None, Some("1MiB")] {
        assert_eq!(
            query_elb(query, memory),
            vec![json!({"bucket":"2019-06-07 18:45:00 +00:00", "n":60})]
        );
    }
}
