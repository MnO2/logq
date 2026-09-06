use serde_json::{Value, json};
use std::process::Command;

fn query(input: &str, sql: &str) -> Value {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("input.jsonl");
    std::fs::write(&path, input).unwrap();
    let output = Command::new(env!("CARGO_BIN_EXE_logq"))
        .args(["query", "--output", "ndjson", "--threads", "1", "--table"])
        .arg(format!("it:jsonl={}", path.display()))
        .arg(sql)
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{sql}: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    serde_json::from_slice(&output.stdout).unwrap()
}

#[test]
fn min_max_ignore_absent_values_and_compare_mixed_numbers() {
    for input in [
        "{\"x\":null}\n{}\n{\"x\":3}\n{\"x\":1.5}\n{\"x\":4.5}\n",
        "{\"x\":1.5}\n{\"x\":null}\n{\"x\":3}\n{\"x\":4.5}\n{}\n",
    ] {
        assert_eq!(
            query(input, "select min(x) as lo, max(x) as hi from it"),
            json!({"lo":1.5,"hi":4.5})
        );
    }
    assert_eq!(
        query("{\"x\":null}\n{}\n", "select min(x) as lo, max(x) as hi from it"),
        json!({"lo":null,"hi":null})
    );
}

#[test]
fn exact_percentile_uses_nearest_rank_and_accepts_endpoints() {
    let input = "{\"x\":10}\n{\"x\":20}\n{\"x\":30}\n{\"x\":40}\n";
    for (p, asc, desc) in [("0.0", 10, 40), ("0.5", 20, 30), ("1.0", 40, 10)] {
        for (order, expected) in [("asc", asc), ("desc", desc)] {
            assert_eq!(
                query(
                    input,
                    &format!("select percentile_disc({p}) within group (order by x {order}) as p from it")
                ),
                json!({"p":expected})
            );
        }
    }
}

#[test]
fn percentiles_ignore_null_and_missing_values() {
    for name in ["percentile_disc", "approx_percentile"] {
        assert_eq!(
            query(
                "{\"x\":null}\n{}\n{\"x\":7}\n",
                &format!("select {name}(0.9) within group (order by x asc) as p from it")
            ),
            json!({"p":7})
        );
        assert_eq!(
            query(
                "{\"x\":null}\n{}\n",
                &format!("select {name}(0.9) within group (order by x asc) as p from it")
            ),
            json!({"p":null})
        );
    }
}

#[test]
fn approximate_percentile_respects_descending_order() {
    let input = "{\"x\":10}\n{\"x\":20}\n{\"x\":30}\n{\"x\":40}\n";
    assert_eq!(
        query(
            input,
            "select approx_percentile(0.0) within group (order by x desc) as p from it"
        ),
        json!({"p":40})
    );
    assert_eq!(
        query(
            input,
            "select approx_percentile(1.0) within group (order by x desc) as p from it"
        ),
        json!({"p":10})
    );
}
