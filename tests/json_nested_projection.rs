use serde_json::{Value, json};
use std::io::Write;
use std::process::{Command, Output, Stdio};

fn query(input: &str, sql: &str, threads: usize, mode: &str) -> Output {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("input.jsonl");
    let bytes = if mode == "gzip" {
        let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::fast());
        encoder.write_all(input.as_bytes()).unwrap();
        encoder.finish().unwrap()
    } else {
        input.as_bytes().to_vec()
    };
    std::fs::write(&path, bytes).unwrap();
    let source = if mode == "stdin" {
        "stdin".into()
    } else {
        path.display().to_string()
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
        .arg(format!("it:jsonl={source}"))
        .arg(sql)
        .stdin(if mode == "stdin" {
            Stdio::from(std::fs::File::open(path).unwrap())
        } else {
            Stdio::null()
        })
        .output()
        .unwrap()
}

fn assert_query(input: &str, sql: &str, expected: &[Value]) {
    for mode in ["stdin", "plain", "gzip"] {
        for threads in [1, 4] {
            let result = query(input, sql, threads, mode);
            assert!(
                result.status.success(),
                "{sql}, {mode}, {threads}: {}",
                String::from_utf8_lossy(&result.stderr)
            );
            let actual: Vec<Value> = String::from_utf8(result.stdout)
                .unwrap()
                .lines()
                .map(|line| serde_json::from_str(line).unwrap())
                .collect();
            assert_eq!(actual, expected, "{sql}, {mode}, {threads}");
        }
    }
}

#[test]
fn nested_projection_preserves_whole_values_and_duplicate_last_wins() {
    let input = r#"{"nested":{"metrics":{"v":1,"other":2},"sibling":3},"nested":{"metrics":{"v":4,"other":5,"v":6},"sibling":7}}
{"nested":{"metrics":{"v":8}},"nested":{"metrics":{"other":9}}}
"#;
    assert_query(
        input,
        "select nested.metrics.v as v from it",
        &[json!({"v":6}), json!({"v":null})],
    );
    assert_query(
        input,
        "select nested.metrics as metrics, nested.metrics.v as v from it",
        &[
            json!({"metrics":{"v":6,"other":5},"v":6}),
            json!({"metrics":{"other":9},"v":null}),
        ],
    );
    assert_query(
        input,
        "select nested as whole, nested.metrics.v as v from it",
        &[
            json!({"whole":{"metrics":{"v":6,"other":5},"sibling":7},"v":6}),
            json!({"whole":{"metrics":{"other":9}},"v":null}),
        ],
    );
}

#[test]
fn nested_projection_preserves_type_drift_arrays_and_wildcards() {
    let input = "{\"nested\":{\"v\":1}}\n{\"nested\":null}\n{\"nested\":3}\n{\"nested\":[{\"v\":4}]}\n{}\n{\"nested\":{\"v\":null}}\n";
    assert_query(
        input,
        "select nested.v as v, nested is missing as absent from it",
        &[
            json!({"v":1,"absent":false}),
            json!({"v":null,"absent":false}),
            json!({"v":null,"absent":false}),
            json!({"v":null,"absent":false}),
            json!({"v":null,"absent":true}),
            json!({"v":null,"absent":false}),
        ],
    );
    assert_query(
        "{\"items\":[{\"v\":1,\"other\":2},{\"v\":3,\"other\":4}]}\n",
        "select items[0] as first, items[*].v as values from it",
        &[json!({"first":{"v":1,"other":2},"values":[1,3]})],
    );
    assert_query(
        "{\"nested\":{\"a\":1,\"b\":2}}\n",
        "select nested.* as values from it",
        &[json!({"values":[1,2]})],
    );
}

#[test]
fn nested_projection_validates_ignored_siblings_and_remains_demand_driven() {
    let invalid = [
        r#"{"nested":{"v":1,"ignored":1e9999}}"#.to_string(),
        r#"{"nested":{"v":1,"ignored":"\uD800"}}"#.to_string(),
        r#"{"nested":{"v":1,"ignored":{"\uD800":0}}}"#.to_string(),
        format!(
            "{{\"nested\":{{\"v\":1,\"ignored\":{}0{}}}}}",
            "[".repeat(130),
            "]".repeat(130)
        ),
    ];
    for input in invalid {
        for mode in ["stdin", "plain", "gzip"] {
            for threads in [1, 4] {
                let result = query(&input, "select nested.v as v from it", threads, mode);
                assert!(
                    !result.status.success(),
                    "accepted invalid JSON: {input}, {mode}, {threads}"
                );
            }
        }
        assert_query(
            &format!("{{\"nested\":{{\"v\":2}}}}\n{input}\n"),
            "select nested.v as v from it limit 1",
            &[json!({"v":2})],
        );
    }
}

#[test]
fn nested_projection_collects_filter_aggregate_and_order_dependencies() {
    let input = "{\"nested\":{\"v\":1,\"keep\":true}}\n{\"nested\":{\"v\":3,\"keep\":true}}\n{\"nested\":{\"v\":9,\"keep\":false}}\n";
    assert_query(
        input,
        "select sum(nested.v) as total from it where nested.keep = true",
        &[json!({"total":4})],
    );
    assert_query(
        input,
        "select nested.v as v from it where nested.keep = true order by v desc",
        &[json!({"v":3}), json!({"v":1})],
    );
}
